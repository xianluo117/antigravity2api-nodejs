import { randomUUID } from "crypto";
import { DEFAULT_REQUEST_COUNT_PER_TOKEN } from "../constants/index.js";
import {
  generateInstanceId,
  generateSessionId,
  generateTokenId,
} from "../utils/idGenerator.js";
import { log } from "../utils/logger.js";
import TokenStore from "./token_store.js";
import * as projectMethods from "./token_manager/project.js";
import * as manageMethods from "./token_manager/manage.js";
import {
  RotationStrategy,
  getRotationProgress,
  getToken,
  loadRotationConfig,
  randomizeRotationStart,
  updateRotationConfig,
  _canUseTokenForModel,
  _checkAllTokensExhaustedForModel,
  _getDefaultStrategyStartIndex,
  _getOrderedCandidateIndices,
  _getTokenForDefaultStrategy,
  _getTokenForQuotaExhaustedStrategy,
  _getTokenModelAvailability,
  _hasQuotaForModel,
  _isTokenAvailableForModel,
  _orderTokenCandidates,
  _rebuildAvailableQuotaTokens,
  _removeQuotaIndex,
  _resetAllQuotas,
} from "./token_manager/rotation.js";

class TokenManager {
  constructor(filePath) {
    this.store = new TokenStore(filePath);
    /** @type {Array<Object>} */
    this.tokens = [];
    /** @type {number} */
    this.currentIndex = 0;

    /** @type {string} */
    this.rotationStrategy = RotationStrategy.ROUND_ROBIN;
    /** @type {number} */
    this.requestCountPerToken = DEFAULT_REQUEST_COUNT_PER_TOKEN;
    /** @type {Map<string, number>} */
    this.tokenRequestCounts = new Map();

    /** @type {number[]} */
    this.availableQuotaTokenIndices = [];
    /** @type {number} */
    this.currentQuotaIndex = 0;

    /** @type {Promise<void>|null} */
    this._initPromise = null;
  }

  async _initialize() {
    try {
      log.info("正在初始化token管理器...");
      const salt = await this.store.getSalt();
      const tokenArray = await this.store.readAll();

      this.tokens = tokenArray
        .filter((token) => token.enable !== false)
        .map((token) => ({
          ...token,
          tokenId: generateTokenId(token.refresh_token, salt),
          sessionId: generateSessionId(),
          instanceId: generateInstanceId(),
          deviceId: randomUUID(),
          sub: token?.sub || "free-tier",
          credits:
            token?.credits ??
            ((token?.sub || "free-tier") === "free-tier" ? 0 : null),
        }));

      this.currentIndex = 0;
      this.tokenRequestCounts.clear();
      this._rebuildAvailableQuotaTokens();
      this.loadRotationConfig();

      if (this.tokens.length === 0) {
        log.warn("⚠ 暂无可用账号，请使用以下方式添加：");
        log.warn("  方式1: 运行 npm run login 命令登录");
        log.warn("  方式2: 访问前端管理页面添加账号");
      } else {
        log.info(`成功加载 ${this.tokens.length} 个可用token`);
        if (this.rotationStrategy === RotationStrategy.REQUEST_COUNT) {
          log.info(
            `轮询策略: ${this.rotationStrategy}, 每token请求 ${this.requestCountPerToken} 次后切换`,
          );
        } else {
          log.info(`轮询策略: ${this.rotationStrategy}`);
        }

        await this._refreshExpiredTokensConcurrently();
      }
    } catch (error) {
      log.error("初始化token失败:", error.message);
      this.tokens = [];
    }
  }
}

Object.assign(TokenManager.prototype, projectMethods, manageMethods, {
  RotationStrategy,
  loadRotationConfig,
  updateRotationConfig,
  _rebuildAvailableQuotaTokens,
  _removeQuotaIndex,
  _resetAllQuotas,
  _checkAllTokensExhaustedForModel,
  _hasQuotaForModel,
  _isTokenAvailableForModel,
  _canUseTokenForModel,
  _getTokenModelAvailability,
  _orderTokenCandidates,
  _getDefaultStrategyStartIndex,
  _getOrderedCandidateIndices,
  randomizeRotationStart,
  getRotationProgress,
  getToken,
  _getTokenForQuotaExhaustedStrategy,
  _getTokenForDefaultStrategy,
});

export { RotationStrategy };

const tokenManager = new TokenManager();
export default tokenManager;
