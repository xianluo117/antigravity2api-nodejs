import { getConfigJson } from "../../config/config.js";
import { generateTokenId } from "../../utils/idGenerator.js";
import { log } from "../../utils/logger.js";
import quotaManager from "../quota_manager.js";
import tokenCooldownManager from "../token_cooldown_manager.js";

export const RotationStrategy = {
  ROUND_ROBIN: "round_robin",
  QUOTA_EXHAUSTED: "quota_exhausted",
  REQUEST_COUNT: "request_count",
};

export function loadRotationConfig() {
  try {
    const jsonConfig = getConfigJson();
    if (jsonConfig.rotation) {
      this.rotationStrategy =
        jsonConfig.rotation.strategy || RotationStrategy.ROUND_ROBIN;
      this.requestCountPerToken = jsonConfig.rotation.requestCount || 10;
    }
  } catch (error) {
    log.warn("加载轮询配置失败，使用默认值:", error.message);
  }
}

export function updateRotationConfig(strategy, requestCount) {
  if (strategy && Object.values(RotationStrategy).includes(strategy)) {
    this.rotationStrategy = strategy;
  }
  if (requestCount && requestCount > 0) {
    this.requestCountPerToken = requestCount;
  }

  this.tokenRequestCounts.clear();
  if (this.rotationStrategy === RotationStrategy.REQUEST_COUNT) {
    log.info(
      `轮询策略已更新: ${this.rotationStrategy}, 每token请求 ${this.requestCountPerToken} 次后切换`,
    );
  } else {
    log.info(`轮询策略已更新: ${this.rotationStrategy}`);
  }
}

export function _rebuildAvailableQuotaTokens() {
  this.availableQuotaTokenIndices = [];
  this.tokens.forEach((token, index) => {
    if (token.enable !== false && token.hasQuota !== false) {
      this.availableQuotaTokenIndices.push(index);
    }
  });

  if (this.availableQuotaTokenIndices.length === 0) {
    this.currentQuotaIndex = 0;
  } else {
    this.currentQuotaIndex =
      this.currentQuotaIndex % this.availableQuotaTokenIndices.length;
  }
}

export function _removeQuotaIndex(tokenIndex) {
  const pos = this.availableQuotaTokenIndices.indexOf(tokenIndex);
  if (pos !== -1) {
    this.availableQuotaTokenIndices.splice(pos, 1);
    if (this.currentQuotaIndex >= this.availableQuotaTokenIndices.length) {
      this.currentQuotaIndex = 0;
    }
  }
}

export function _resetAllQuotas() {
  log.warn("所有token额度已耗尽，重置额度状态");
  this.tokens.forEach((token) => {
    token.hasQuota = true;
  });
  this.saveToFile();
  this._rebuildAvailableQuotaTokens();
}

export function _checkAllTokensExhaustedForModel(modelId) {
  if (!modelId || this.tokens.length === 0) return false;

  for (const token of this.tokens) {
    if (this._canUseTokenForModel(token, modelId)) {
      return false;
    }
  }
  return true;
}

export function _hasQuotaForModel(token, modelId) {
  if (!token || !modelId) return true;

  try {
    const salt = this.store._salt;
    if (!salt) return true;

    const tokenId = generateTokenId(token.refresh_token, salt);
    return quotaManager.hasQuotaForModel(tokenId, modelId);
  } catch {
    return true;
  }
}

export function _isTokenAvailableForModel(token, modelId) {
  if (!token || !modelId) return true;

  try {
    const salt = this.store._salt;
    if (!salt) return true;

    const tokenId = generateTokenId(token.refresh_token, salt);
    return tokenCooldownManager.isAvailable(tokenId, modelId);
  } catch {
    return true;
  }
}

export function _canUseTokenForModel(token, modelId) {
  if (!token || !modelId) return true;

  if (!this._isTokenAvailableForModel(token, modelId)) {
    return false;
  }

  return this._hasQuotaForModel(token, modelId);
}

export function _getTokenModelAvailability(token, modelId) {
  if (!token || !modelId) {
    return {
      hasData: false,
      estimatedRequests: null,
      canUse: true,
      priority: 0,
    };
  }

  if (!this._isTokenAvailableForModel(token, modelId)) {
    return {
      hasData: true,
      estimatedRequests: 0,
      canUse: false,
      priority: Number.MAX_SAFE_INTEGER,
    };
  }

  try {
    const salt = this.store._salt;
    if (!salt) {
      return {
        hasData: false,
        estimatedRequests: null,
        canUse: true,
        priority: 2,
      };
    }

    const tokenId = generateTokenId(token.refresh_token, salt);
    const availability = quotaManager.getModelGroupAvailability(tokenId, modelId);

    if (!availability.hasData) {
      return {
        hasData: false,
        estimatedRequests: null,
        canUse: true,
        priority: 2,
      };
    }

    const estimatedRequests = availability.estimatedRequests || 0;
    const requiredBudget =
      this.rotationStrategy === RotationStrategy.REQUEST_COUNT
        ? Math.max(1, this.requestCountPerToken)
        : 1;

    return {
      hasData: true,
      estimatedRequests,
      canUse: estimatedRequests > 0,
      priority: estimatedRequests >= requiredBudget ? 0 : 1,
    };
  } catch {
    return {
      hasData: false,
      estimatedRequests: null,
      canUse: true,
      priority: 2,
    };
  }
}

export function _orderTokenCandidates(candidateIndices, modelId = null) {
  const ordered = candidateIndices
    .map((tokenIndex, order) => ({
      tokenIndex,
      order,
      availability: this._getTokenModelAvailability(
        this.tokens[tokenIndex],
        modelId,
      ),
    }))
    .filter((item) => item.availability.canUse);

  if (!modelId) {
    return ordered;
  }

  const knownAvailable = ordered.filter((item) => item.availability.hasData);
  const source = knownAvailable.length > 0 ? knownAvailable : ordered;

  return source.sort((a, b) => {
    if (a.availability.priority !== b.availability.priority) {
      return a.availability.priority - b.availability.priority;
    }

    const aEstimated = a.availability.estimatedRequests;
    const bEstimated = b.availability.estimatedRequests;
    if (
      aEstimated !== null &&
      bEstimated !== null &&
      aEstimated !== bEstimated
    ) {
      return bEstimated - aEstimated;
    }

    return a.order - b.order;
  });
}

export function _getDefaultStrategyStartIndex() {
  const totalTokens = this.tokens.length;
  if (totalTokens === 0) return 0;

  let startIndex = this.currentIndex % totalTokens;

  if (this.rotationStrategy === RotationStrategy.REQUEST_COUNT) {
    const currentToken = this.tokens[startIndex];
    const tokenKey = currentToken?.refresh_token;
    const count = tokenKey ? this.tokenRequestCounts.get(tokenKey) || 0 : 0;

    if (tokenKey && count >= this.requestCountPerToken) {
      startIndex = (startIndex + 1) % totalTokens;
    }
  }

  return startIndex;
}

export function _getOrderedCandidateIndices(modelId = null) {
  if (this.tokens.length === 0) return [];

  const candidateIndices = [];

  if (this.rotationStrategy === RotationStrategy.QUOTA_EXHAUSTED) {
    const totalAvailable = this.availableQuotaTokenIndices.length;
    if (totalAvailable === 0) return [];

    const startIndex = this.currentQuotaIndex % totalAvailable;
    for (let i = 0; i < totalAvailable; i++) {
      const listIndex = (startIndex + i) % totalAvailable;
      candidateIndices.push(this.availableQuotaTokenIndices[listIndex]);
    }
  } else {
    const totalTokens = this.tokens.length;
    const startIndex = this._getDefaultStrategyStartIndex();
    for (let i = 0; i < totalTokens; i++) {
      candidateIndices.push((startIndex + i) % totalTokens);
    }
  }

  const allTokensExhausted = modelId
    ? this._checkAllTokensExhaustedForModel(modelId)
    : false;

  if (modelId && !allTokensExhausted) {
    return this._orderTokenCandidates(candidateIndices, modelId).map(
      (item) => item.tokenIndex,
    );
  }

  return candidateIndices;
}

export function randomizeRotationStart(preferredIndices = []) {
  if (this.tokens.length === 0) {
    this.currentIndex = 0;
    this.currentQuotaIndex = 0;
    return null;
  }

  const normalizedPreferred = Array.from(
    new Set(
      (preferredIndices || []).filter(
        (index) =>
          Number.isInteger(index) && index >= 0 && index < this.tokens.length,
      ),
    ),
  );

  if (this.rotationStrategy === RotationStrategy.QUOTA_EXHAUSTED) {
    this._rebuildAvailableQuotaTokens();
    const quotaCandidates =
      normalizedPreferred.length > 0
        ? this.availableQuotaTokenIndices.filter((index) =>
            normalizedPreferred.includes(index),
          )
        : [...this.availableQuotaTokenIndices];

    const source =
      quotaCandidates.length > 0
        ? quotaCandidates
        : [...this.availableQuotaTokenIndices];

    if (source.length === 0) {
      this.currentQuotaIndex = 0;
      this.currentIndex = 0;
      return null;
    }

    const targetTokenIndex =
      source[Math.floor(Math.random() * source.length)] || source[0];
    const listIndex = this.availableQuotaTokenIndices.indexOf(targetTokenIndex);
    this.currentQuotaIndex = listIndex >= 0 ? listIndex : 0;
    this.currentIndex = targetTokenIndex;
    return targetTokenIndex;
  }

  const source =
    normalizedPreferred.length > 0
      ? normalizedPreferred
      : this.tokens.map((_, index) => index);

  const targetTokenIndex =
    source[Math.floor(Math.random() * source.length)] ?? source[0] ?? 0;
  this.currentIndex = targetTokenIndex;
  return targetTokenIndex;
}

export function getRotationProgress(groups = {}) {
  const progress = {};

  Object.entries(groups).forEach(([groupKey, groupConfig]) => {
    const candidateIndices = this._getOrderedCandidateIndices(groupConfig.modelId);
    const tokenIndex = candidateIndices.length > 0 ? candidateIndices[0] : null;
    const token = tokenIndex !== null ? this.tokens[tokenIndex] : null;
    const tokenKey = token?.refresh_token || null;
    const currentRequestCount = tokenKey
      ? this.tokenRequestCounts.get(tokenKey) || 0
      : 0;

    progress[groupKey] = {
      label: groupConfig.label,
      modelId: groupConfig.modelId,
      currentIndex: tokenIndex,
      currentTokenId: token?.tokenId || null,
      currentPosition: tokenIndex === null ? null : tokenIndex + 1,
      totalTokens: this.tokens.length,
      candidateCount: candidateIndices.length,
      currentRequestCount,
      requestCountTarget:
        this.rotationStrategy === RotationStrategy.REQUEST_COUNT
          ? this.requestCountPerToken
          : null,
      remainingToSwitch:
        this.rotationStrategy === RotationStrategy.REQUEST_COUNT
          ? Math.max(0, this.requestCountPerToken - currentRequestCount)
          : null,
    };
  });

  return progress;
}

export async function getToken(modelId = null) {
  await this._ensureInitialized();
  if (this.tokens.length === 0) return null;

  if (this.rotationStrategy === RotationStrategy.QUOTA_EXHAUSTED) {
    return this._getTokenForQuotaExhaustedStrategy(modelId);
  }

  return this._getTokenForDefaultStrategy(modelId);
}

export async function _getTokenForQuotaExhaustedStrategy(modelId = null) {
  if (this.availableQuotaTokenIndices.length === 0) {
    this._resetAllQuotas();
  }

  const totalAvailable = this.availableQuotaTokenIndices.length;
  if (totalAvailable === 0) {
    return null;
  }

  let allTokensExhausted = false;
  if (modelId) {
    allTokensExhausted = this._checkAllTokensExhaustedForModel(modelId);
  }

  const startIndex = this.currentQuotaIndex % totalAvailable;
  const candidateIndices = [];
  const quotaListIndices = new Map();

  for (let i = 0; i < totalAvailable; i++) {
    const listIndex = (startIndex + i) % totalAvailable;
    const tokenIndex = this.availableQuotaTokenIndices[listIndex];
    candidateIndices.push(tokenIndex);
    quotaListIndices.set(tokenIndex, listIndex);
  }

  const candidateOrder =
    modelId && !allTokensExhausted
      ? this._orderTokenCandidates(candidateIndices, modelId)
      : candidateIndices.map((tokenIndex) => ({
          tokenIndex,
          availability: {
            hasData: false,
            estimatedRequests: null,
            canUse: true,
            priority: 0,
          },
        }));

  for (const candidate of candidateOrder) {
    const tokenIndex = candidate.tokenIndex;
    const token = this.tokens[tokenIndex];
    const listIndex = quotaListIndices.get(tokenIndex) ?? 0;

    try {
      const result = await this._prepareToken(token);
      if (result.action === "disable") {
        this.disableToken(token, result.reason);
        this._rebuildAvailableQuotaTokens();
        if (
          this.tokens.length === 0 ||
          this.availableQuotaTokenIndices.length === 0
        ) {
          return null;
        }
        continue;
      }

      this.currentIndex = tokenIndex;
      this.currentQuotaIndex = listIndex;
      return token;
    } catch (error) {
      const errorResult = this._handleTokenError(error, token);
      if (errorResult.action === "disable") {
        this.disableToken(token, errorResult.reason);
        this._rebuildAvailableQuotaTokens();
        if (
          this.tokens.length === 0 ||
          this.availableQuotaTokenIndices.length === 0
        ) {
          return null;
        }
      }
    }
  }

  this._resetAllQuotas();
  return this.tokens[0] || null;
}

export async function _getTokenForDefaultStrategy(modelId = null) {
  const totalTokens = this.tokens.length;
  let startIndex = this.currentIndex;

  if (
    this.rotationStrategy === RotationStrategy.REQUEST_COUNT &&
    totalTokens > 0
  ) {
    const currentToken = this.tokens[startIndex];
    const tokenKey = currentToken?.refresh_token;
    const count = tokenKey ? this.tokenRequestCounts.get(tokenKey) || 0 : 0;

    if (tokenKey && count >= this.requestCountPerToken) {
      this.resetRequestCount(tokenKey);
      startIndex = (startIndex + 1) % totalTokens;
      this.currentIndex = startIndex;
    }
  }

  let allTokensExhausted = false;
  if (modelId) {
    allTokensExhausted = this._checkAllTokensExhaustedForModel(modelId);
  }

  const candidateIndices = [];
  for (let i = 0; i < totalTokens; i++) {
    candidateIndices.push((startIndex + i) % totalTokens);
  }

  const candidateOrder =
    modelId && !allTokensExhausted
      ? this._orderTokenCandidates(candidateIndices, modelId)
      : candidateIndices.map((tokenIndex) => ({
          tokenIndex,
          availability: {
            hasData: false,
            estimatedRequests: null,
            canUse: true,
            priority: 0,
          },
        }));

  for (const candidate of candidateOrder) {
    const index = candidate.tokenIndex;
    const token = this.tokens[index];

    try {
      const result = await this._prepareToken(token);
      if (result.action === "disable") {
        this.disableToken(token, result.reason);
        if (this.tokens.length === 0) return null;
        continue;
      }

      this.currentIndex = index;

      if (this.rotationStrategy === RotationStrategy.ROUND_ROBIN) {
        this.currentIndex = (this.currentIndex + 1) % this.tokens.length;
      }

      return token;
    } catch (error) {
      const errorResult = this._handleTokenError(error, token);
      if (errorResult.action === "disable") {
        this.disableToken(token, errorResult.reason);
        if (this.tokens.length === 0) return null;
      }
    }
  }

  return null;
}
