import { generateTokenId } from "../../utils/idGenerator.js";
import quotaManager from "../quota_manager.js";
import tokenCooldownManager from "../token_cooldown_manager.js";

const REQUEST_COUNT_STRATEGY = "request_count";

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
    return quotaManager.hasQuotaForModel(tokenId, modelId, "geminicli");
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
    return tokenCooldownManager.isAvailable(tokenId, modelId, "geminicli");
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
    const availability = quotaManager.getModelGroupAvailability(
      tokenId,
      modelId,
      "geminicli",
    );

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
      this.rotationStrategy === REQUEST_COUNT_STRATEGY
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
      availability: this._getTokenModelAvailability(this.tokens[tokenIndex], modelId),
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
