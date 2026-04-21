import { getConfigJson } from "../../config/config.js";
import { log } from "../../utils/logger.js";

export {
  _canUseTokenForModel,
  _checkAllTokensExhaustedForModel,
  _getTokenModelAvailability,
  _hasQuotaForModel,
  _isTokenAvailableForModel,
  _orderTokenCandidates,
} from "./model_availability.js";

export const RotationStrategy = {
  ROUND_ROBIN: "round_robin",
  QUOTA_EXHAUSTED: "quota_exhausted",
  REQUEST_COUNT: "request_count",
};

function createFallbackCandidateOrder(candidateIndices = []) {
  return candidateIndices.map((tokenIndex) => ({
    tokenIndex,
    availability: {
      hasData: false,
      estimatedRequests: null,
      canUse: true,
      priority: 0,
    },
  }));
}

export function loadRotationConfig() {
  try {
    const jsonConfig = getConfigJson();
    const rotationConfig = jsonConfig.geminicli?.rotation || jsonConfig.rotation;
    if (rotationConfig) {
      this.rotationStrategy =
        rotationConfig.strategy || RotationStrategy.ROUND_ROBIN;
      this.requestCountPerToken = rotationConfig.requestCount || 10;
    }
  } catch (error) {
    log.warn("[GeminiCLI] 加载轮询配置失败，使用默认值:", error.message);
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
      `[GeminiCLI] 轮询策略已更新: ${this.rotationStrategy}, 每token请求 ${this.requestCountPerToken} 次后切换`,
    );
  } else {
    log.info(`[GeminiCLI] 轮询策略已更新: ${this.rotationStrategy}`);
  }
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

  const totalTokens = this.tokens.length;
  const startIndex = this._getDefaultStrategyStartIndex();
  const candidateIndices = [];
  for (let i = 0; i < totalTokens; i++) {
    candidateIndices.push((startIndex + i) % totalTokens);
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

export function randomizeRotationStart(preferredIndices = []) {
  if (this.tokens.length === 0) {
    this.currentIndex = 0;
    return null;
  }

  const source = Array.from(
    new Set(
      (preferredIndices && preferredIndices.length > 0
        ? preferredIndices
        : this.tokens.map((_, index) => index)
      ).filter(
        (index) =>
          Number.isInteger(index) && index >= 0 && index < this.tokens.length,
      ),
    ),
  );

  if (source.length === 0) {
    this.currentIndex = 0;
    return null;
  }

  const targetTokenIndex =
    source[Math.floor(Math.random() * source.length)] ?? source[0] ?? 0;
  this.currentIndex = targetTokenIndex;
  return targetTokenIndex;
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
      : createFallbackCandidateOrder(candidateIndices);

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

export async function getToken(modelId = null) {
  await this._ensureInitialized();
  if (this.tokens.length === 0) return null;

  return this._getTokenForDefaultStrategy(modelId);
}
