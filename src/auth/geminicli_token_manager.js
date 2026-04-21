import path from "path";
import config from "../config/config.js";
import { DEFAULT_REQUEST_COUNT_PER_TOKEN } from "../constants/index.js";
import { TokenError } from "../utils/errors.js";
import { httpRequest } from "../utils/httpClient.js";
import { generateTokenId } from "../utils/idGenerator.js";
import { log } from "../utils/logger.js";
import { getDataDir } from "../utils/paths.js";
import * as lifecycleMethods from "./geminicli_token_manager/lifecycle.js";
import * as projectMethods from "./geminicli_token_manager/project.js";
import {
  RotationStrategy,
  _canUseTokenForModel,
  _checkAllTokensExhaustedForModel,
  _getDefaultStrategyStartIndex,
  _getOrderedCandidateIndices,
  _getTokenForDefaultStrategy,
  _getTokenModelAvailability,
  _hasQuotaForModel,
  _isTokenAvailableForModel,
  _orderTokenCandidates,
  getRotationProgress,
  getToken,
  loadRotationConfig,
  randomizeRotationStart,
  updateRotationConfig,
} from "./geminicli_token_manager/rotation.js";
import quotaManager from "./quota_manager.js";
import TokenStore from "./token_store.js";

/**
 * Gemini CLI Token 管理器
 * 基于 TokenManager 简化实现，专门用于 Gemini CLI 反代
 * 主要区别：
 * 1. 使用 geminicli_accounts.json 存储
 * 2. 使用 GEMINICLI_OAUTH_CONFIG 刷新 token
 * 3. 不需要 projectId 和 sessionId
 */
class GeminiCliTokenManager {
  /**
   * @param {string} filePath - Token 数据文件路径
   */
  constructor(filePath = path.join(getDataDir(), "geminicli_accounts.json")) {
    this.store = new TokenStore(filePath);
    /** @type {Array<Object>} */
    this.tokens = [];
    /** @type {number} */
    this.currentIndex = 0;

    // 轮询策略相关
    /** @type {string} */
    this.rotationStrategy = RotationStrategy.ROUND_ROBIN;
    /** @type {number} */
    this.requestCountPerToken = DEFAULT_REQUEST_COUNT_PER_TOKEN;
    /** @type {Map<string, number>} */
    this.tokenRequestCounts = new Map();

    /** @type {Promise<void>|null} */
    this._initPromise = null;
  }

  async _initialize() {
    try {
      log.info("[GeminiCLI] 正在初始化token管理器...");
      const salt = await this.store.getSalt();
      const tokenArray = await this.store.readAll();

      // Gemini CLI 不需要 sessionId
      this.tokens = tokenArray
        .filter((token) => token.enable !== false)
        .map((token) => ({
          ...token,
          tokenId: generateTokenId(token.refresh_token, salt),
        }));

      this.currentIndex = 0;
      this.tokenRequestCounts.clear();

      // 加载轮询策略配置
      this.loadRotationConfig();

      if (this.tokens.length === 0) {
        log.warn("[GeminiCLI] ⚠ 暂无可用账号，请使用以下方式添加：");
        log.warn("[GeminiCLI]   方式1: 访问前端管理页面添加账号");
        log.warn("[GeminiCLI]   方式2: 手动编辑 geminicli_accounts.json");
      } else {
        log.info(`[GeminiCLI] 成功加载 ${this.tokens.length} 个可用token`);
        if (this.rotationStrategy === RotationStrategy.REQUEST_COUNT) {
          log.info(
            `[GeminiCLI] 轮询策略: ${this.rotationStrategy}, 每token请求 ${this.requestCountPerToken} 次后切换`,
          );
        } else {
          log.info(`[GeminiCLI] 轮询策略: ${this.rotationStrategy}`);
        }

        // 并发刷新所有过期的 token
        await this._refreshExpiredTokensConcurrently();
      }
    } catch (error) {
      log.error("[GeminiCLI] 初始化token失败:", error.message);
      this.tokens = [];
    }
  }

  // lifecycle / project / rotation 相关方法通过 Object.assign 挂载到原型

  saveToFile(tokenToUpdate = null) {
    this.store.mergeActiveTokens(this.tokens, tokenToUpdate).catch((error) => {
      log.error("[GeminiCLI] 保存账号配置文件失败:", error.message);
    });
  }

  disableToken(token, reason = "未知原因") {
    log.warn(
      `[GeminiCLI] 禁用token ...${token.access_token.slice(-8)}, 原因: ${reason}`,
    );
    token.enable = false;
    token.disableReason = reason;
    token.disableTime = Date.now();
    token.lastError = reason;
    token.lastErrorTime = token.disableTime;
    token.lastErrorStage = "disable";
    this.saveToFile();
    this.tokenRequestCounts.delete(token.refresh_token);
    this.tokens = this.tokens.filter(
      (t) => t.refresh_token !== token.refresh_token,
    );
    this.currentIndex = this.currentIndex % Math.max(this.tokens.length, 1);
  }

  // 原子操作：获取并递增请求计数
  incrementRequestCount(tokenKey) {
    const current = this.tokenRequestCounts.get(tokenKey) || 0;
    const newCount = current + 1;
    this.tokenRequestCounts.set(tokenKey, newCount);
    return newCount;
  }

  // 原子操作：重置请求计数
  resetRequestCount(tokenKey) {
    this.tokenRequestCounts.set(tokenKey, 0);
  }

  /**
   * 记录一次请求（用于 request_count 轮询及额度预估）
   * @param {Object} token - Token 对象
   * @param {string} modelId - 模型 ID
   */
  async recordRequest(token, modelId) {
    if (!token?.refresh_token) return;

    this.incrementRequestCount(token.refresh_token);

    if (!modelId) return;

    try {
      const salt = await this.store.getSalt();
      const tokenId = generateTokenId(token.refresh_token, salt);
      quotaManager.recordRequest(tokenId, modelId, "geminicli");
    } catch (error) {
      log.warn("[GeminiCLI] 记录请求次数失败:", error.message);
    }
  }



  disableCurrentToken(token, reason = "API请求返回403，账号无使用权限") {
    const found = this.tokens.find(
      (t) => t.access_token === token.access_token,
    );
    if (found) {
      this.disableToken(found, reason);
    }
  }

  // API管理方法
  async reload() {
    this._initPromise = this._initialize();
    await this._initPromise;
    log.info("[GeminiCLI] Token已热重载");
  }

  async addToken(tokenData) {
    try {
      const allTokens = await this.store.readAll();
      const salt = await this.store.getSalt();
      const tokenId = generateTokenId(tokenData.refresh_token, salt);

      const newToken = {
        access_token: tokenData.access_token,
        refresh_token: tokenData.refresh_token,
        expires_in: tokenData.expires_in || 3599,
        timestamp: tokenData.timestamp || Date.now(),
        enable: false,
      };

      if (tokenData.email) {
        newToken.email = tokenData.email;
      }

      if (tokenData.projectId) {
        newToken.projectId = tokenData.projectId;
      }

      if (tokenData.tier) {
        newToken.tier = tokenData.tier;
      }

      allTokens.push(newToken);
      await this.store.writeAll(allTokens);

      await this.reload();
      const enableResult = await this.enableTokenById(tokenId, {
        stage: "oauth_submit",
      });
      if (!enableResult.success) {
        return {
          success: true,
          saved: true,
          validated: false,
          disabled: true,
          tokenId,
          message: enableResult.message || "Token已保存到禁用池",
        };
      }

      return {
        success: true,
        saved: true,
        validated: true,
        disabled: false,
        tokenId,
        message: "Token添加成功",
      };
    } catch (error) {
      log.error("[GeminiCLI] 添加Token失败:", error.message);
      return { success: false, message: error.message };
    }
  }

  async updateToken(refreshToken, updates) {
    try {
      const allTokens = await this.store.readAll();

      const index = allTokens.findIndex(
        (t) => t.refresh_token === refreshToken,
      );
      if (index === -1) {
        return { success: false, message: "Token不存在" };
      }

      // 重新启用时清除禁用原因
      if (updates.enable === true) {
        updates.disableReason = null;
        updates.disableTime = null;
      }

      allTokens[index] = { ...allTokens[index], ...updates };
      await this.store.writeAll(allTokens);

      await this.reload();
      return { success: true, message: "Token更新成功" };
    } catch (error) {
      log.error("[GeminiCLI] 更新Token失败:", error.message);
      return { success: false, message: error.message };
    }
  }

  async deleteToken(refreshToken) {
    try {
      const allTokens = await this.store.readAll();

      const filteredTokens = allTokens.filter(
        (t) => t.refresh_token !== refreshToken,
      );
      if (filteredTokens.length === allTokens.length) {
        return { success: false, message: "Token不存在" };
      }

      await this.store.writeAll(filteredTokens);

      await this.reload();
      return { success: true, message: "Token删除成功" };
    } catch (error) {
      log.error("[GeminiCLI] 删除Token失败:", error.message);
      return { success: false, message: error.message };
    }
  }

  async getTokenList() {
    try {
      const allTokens = await this.store.readAll();
      const salt = await this.store.getSalt();

      return allTokens.map((token) => ({
        id: generateTokenId(token.refresh_token, salt),
        access_token: token.access_token || null,
        refresh_token: token.refresh_token || null,
        expires_in: token.expires_in,
        timestamp: token.timestamp,
        enable: token.enable !== false,
        email: token.email || null,
        projectId: token.projectId || null,
        tier: token.tier || null,
        disableReason: token.disableReason || null,
        disableTime: token.disableTime || null,
        lastError: token.lastError || null,
        lastErrorTime: token.lastErrorTime || null,
        lastErrorStage: token.lastErrorStage || null,
      }));
    } catch (error) {
      log.error("[GeminiCLI] 获取Token列表失败:", error.message);
      return [];
    }
  }

  /**
   * 批量获取已启用 Gemini CLI Token 的 Project ID
   * @returns {Promise<Object>} 批量处理结果
   */
  async batchFetchProjectIds() {
    const allTokens = await this.store.readAll();
    const salt = await this.store.getSalt();
    const enabledTokens = allTokens.filter((token) => token.enable !== false);

    const results = [];
    let successCount = 0;
    let failCount = 0;

    for (const token of enabledTokens) {
      const currentTokenId = generateTokenId(token.refresh_token, salt);
      try {
        const result = await this.fetchProjectIdForTokenData(
          token,
          currentTokenId,
        );
        successCount += 1;
        results.push({
          tokenId: currentTokenId,
          success: true,
          projectId: result.projectId,
          tier: result.tier || null,
        });
      } catch (error) {
        failCount += 1;
        results.push({
          tokenId: currentTokenId,
          success: false,
          message: error.message,
          status: error.statusCode || error.status || 500,
        });
      }
    }

    return {
      total: enabledTokens.length,
      successCount,
      failCount,
      results,
    };
  }

  /**
   * 根据 tokenId 查找完整的 token 对象
   * @param {string} tokenId - 安全的 token ID
   * @returns {Promise<Object|null>} token 对象或 null
   */
  async findTokenById(tokenId) {
    try {
      const allTokens = await this.store.readAll();
      const salt = await this.store.getSalt();

      return (
        allTokens.find(
          (token) => generateTokenId(token.refresh_token, salt) === tokenId,
        ) || null
      );
    } catch (error) {
      log.error("[GeminiCLI] 查找Token失败:", error.message);
      return null;
    }
  }

  /**
   * 根据 tokenId 更新 token
   * @param {string} tokenId - 安全的 token ID
   * @param {Object} updates - 更新内容
   * @returns {Promise<Object>} 操作结果
   */
  async updateTokenById(tokenId, updates) {
    try {
      const allTokens = await this.store.readAll();
      const salt = await this.store.getSalt();

      const index = allTokens.findIndex(
        (token) => generateTokenId(token.refresh_token, salt) === tokenId,
      );

      if (index === -1) {
        return { success: false, message: "Token不存在" };
      }

      // 重新启用时清除禁用原因
      if (updates.enable === true) {
        updates.disableReason = null;
        updates.disableTime = null;
        updates.lastError = null;
        updates.lastErrorTime = null;
        updates.lastErrorStage = null;
      }

      allTokens[index] = { ...allTokens[index], ...updates };
      await this.store.writeAll(allTokens);

      await this.reload();
      return { success: true, message: "Token更新成功" };
    } catch (error) {
      log.error("[GeminiCLI] 更新Token失败:", error.message);
      return { success: false, message: error.message };
    }
  }

  /**
   * 发送测试消息验证凭证在 API 层面的可用性
   * 通过非流式 generateContent 发送一个极简请求，检查是否会返回 403 等致命错误
   * @param {Object} token - Token 对象（必须已刷新且包含 projectId）
   * @returns {Promise<{ok: boolean, status?: number, message?: string}>}
   * @private
   */
  async _sendTestMessage(token) {
    const geminicliConfig = config.geminicli?.api || {};
    const noStreamUrl =
      geminicliConfig.noStreamUrl ||
      "https://cloudcode-pa.googleapis.com/v1internal:generateContent";

    const sendRequest = async () => {
      const testRequestBody = {
        model: "gemini-2.5-flash",
        project: token.projectId,
        request: {
          contents: [{ role: "user", parts: [{ text: "hi" }] }],
          generationConfig: {
            maxOutputTokens: 1,
            candidateCount: 1,
          },
        },
      };

      return httpRequest({
        method: "POST",
        url: noStreamUrl,
        headers: {
          Host: geminicliConfig.host || GEMINICLI_API_CONFIG.HOST,
          "User-Agent":
            geminicliConfig.userAgent || GEMINICLI_API_CONFIG.USER_AGENT,
          Authorization: `Bearer ${token.access_token}`,
          "Content-Type": "application/json",
          "Accept-Encoding": "gzip",
        },
        data: testRequestBody,
        timeout: 30000,
      });
    };

    const shouldRetryWithProjectId = (status, message) => {
      const text = String(message || "").toLowerCase();
      return (
        status === 400 &&
        (text.includes("projectid") ||
          text.includes("project id") ||
          text.includes("project is required") ||
          text.includes("缺少 projectid") ||
          text.includes("缺少 project id") ||
          text.includes("无法获取 projectid"))
      );
    };

    try {
      if (!token.projectId) {
        const result = await this.fetchProjectId(token);
        if (result?.projectId) {
          token.projectId = result.projectId;
        }
        if (result?.tier) {
          token.tier = result.tier;
        }
      }

      await sendRequest();
      return { ok: true };
    } catch (error) {
      const status =
        error.response?.status || error.status || error.statusCode || 500;
      let errorBody = "";
      try {
        const data = error.response?.data;
        errorBody =
          typeof data === "string"
            ? data
            : data
              ? JSON.stringify(data)
              : error.message;
      } catch {
        errorBody = error.message || "未知错误";
      }

      const errorText = String(errorBody || "");

      if (shouldRetryWithProjectId(status, errorText)) {
        try {
          log.info(
            "[GeminiCLI] 启动检测遇到 projectId 缺失，尝试自动获取后重试",
          );
          const result = await this.fetchProjectId(token);
          if (result?.projectId) {
            token.projectId = result.projectId;
          }
          if (result?.tier) {
            token.tier = result.tier;
          }
          if (token.projectId) {
            await sendRequest();
            return { ok: true };
          }
        } catch (retryError) {
          log.warn(
            `[GeminiCLI] 自动获取 projectId 后重试失败: ${retryError.message}`,
          );
        }
      }

      const isContextLimit =
        status === 403 && errorText.includes("The caller does not");
      const shouldDisable =
        (status === 400 || status === 401 || status === 403) && !isContextLimit;

      if (shouldDisable) {
        return { ok: false, status, message: errorText };
      }

      // 其他非致命状态码不阻止启用
      return { ok: true };
    }
  }

  /**
   * 从禁用池启用 token（先测试可用性）
   * @param {string} tokenId - 安全的 token ID
   * @returns {Promise<Object>} 操作结果
   */
  async enableTokenById(tokenId, options = {}) {
    try {
      const tokenData = await this.findTokenById(tokenId);
      const errorStage = options.stage || "enable_test";

      // 辅助函数：将启用验证失败的错误信息写入凭证存储
      const saveEnableError = async (errorMessage) => {
        try {
          const allTokens = await this.store.readAll();
          const salt = await this.store.getSalt();
          const index = allTokens.findIndex(
            (token) => generateTokenId(token.refresh_token, salt) === tokenId,
          );
          if (index !== -1) {
            const errorTime = Date.now();
            allTokens[index] = {
              ...allTokens[index],
              enable: false,
              disableReason: errorMessage,
              disableTime: errorTime,
              lastError: errorMessage,
              lastErrorTime: errorTime,
              lastErrorStage: errorStage,
            };
            await this.store.writeAll(allTokens);
            await this.reload();
          }
        } catch (e) {
          log.error(`[GeminiCLI][启用检测] 保存错误信息失败: ${e.message}`);
        }
      };

      if (!tokenData) {
        return { success: false, message: "Token不存在" };
      }

      // 如果 token 已经启用，直接返回
      if (tokenData.enable !== false) {
        return { success: true, message: "Token已处于启用状态" };
      }

      log.info(`[GeminiCLI][启用检测] 正在测试 token ${tokenId} 的可用性...`);

      // 步骤1: 尝试刷新 token
      try {
        await this.refreshToken(tokenData);
      } catch (error) {
        const statusCode = error.statusCode || 500;
        if (statusCode === 403 || statusCode === 400) {
          log.warn(
            `[GeminiCLI][启用检测] token ${tokenId} 刷新失败(${statusCode}): ${error.message}`,
          );
          const msg = `凭证不可用，刷新失败(${statusCode}): ${error.message}`;
          await saveEnableError(msg);
          return {
            success: false,
            message: msg,
          };
        }
        log.warn(
          `[GeminiCLI][启用检测] token ${tokenId} 刷新失败: ${error.message}`,
        );
        const refreshMsg = `凭证刷新失败: ${error.message}`;
        await saveEnableError(refreshMsg);
        return { success: false, message: refreshMsg };
      }

      // 步骤2: 尝试获取 projectId（验证账号权限）
      try {
        const result = await this.fetchProjectId(tokenData);
        const fetchedProjectId = result?.projectId;
        const fetchedTier = result?.tier;
        if (fetchedProjectId) {
          tokenData.projectId = fetchedProjectId;
          if (fetchedTier) {
            tokenData.tier = fetchedTier;
          }
        } else if (!tokenData.projectId) {
          log.warn(`[GeminiCLI][启用检测] token ${tokenId} 无法获取 projectId`);
          const noProjectMsg =
            "凭证不可用: 无法获取 projectId，该账号可能不支持 Gemini CLI";
          await saveEnableError(noProjectMsg);
          return {
            success: false,
            message: noProjectMsg,
          };
        }
      } catch (error) {
        const statusCode = error.statusCode || 500;
        if (statusCode === 403 || statusCode === 401) {
          log.warn(
            `[GeminiCLI][启用检测] token ${tokenId} 权限验证失败(${statusCode}): ${error.message}`,
          );
          const permMsg = `凭证不可用，权限验证失败(${statusCode}): ${error.message}`;
          await saveEnableError(permMsg);
          return {
            success: false,
            message: permMsg,
          };
        }
        // 非致命错误，继续启用
        log.warn(
          `[GeminiCLI][启用检测] token ${tokenId} 获取 projectId 时出现非致命错误: ${error.message}，继续启用`,
        );
      }

      // 步骤3: 发送测试消息，验证 API 调用是否会触发 403
      if (tokenData.projectId) {
        log.info(`[GeminiCLI][启用检测] 正在发送测试消息验证 API 可用性...`);
        const testResult = await this._sendTestMessage(tokenData);
        if (!testResult.ok) {
          log.warn(
            `[GeminiCLI][启用检测] token ${tokenId} 测试消息失败(${testResult.status}): ${testResult.message}`,
          );
          const testMsg = `凭证不可用，API 测试失败(${testResult.status}): ${testResult.message}`;
          await saveEnableError(testMsg);
          return {
            success: false,
            message: testMsg,
          };
        }
        log.info(`[GeminiCLI][启用检测] token ${tokenId} 测试消息通过`);
      }

      log.info(
        `[GeminiCLI][启用检测] token ${tokenId} 全部检测通过，正在启用...`,
      );

      // 测试通过，执行启用
      const allTokens = await this.store.readAll();
      const salt = await this.store.getSalt();

      const index = allTokens.findIndex(
        (token) => generateTokenId(token.refresh_token, salt) === tokenId,
      );

      if (index === -1) {
        return { success: false, message: "Token不存在" };
      }

      const updates = {
        enable: true,
        disableReason: null,
        disableTime: null,
        lastError: null,
        lastErrorTime: null,
        lastErrorStage: null,
      };
      if (tokenData.projectId) updates.projectId = tokenData.projectId;
      if (tokenData.tier) updates.tier = tokenData.tier;

      allTokens[index] = { ...allTokens[index], ...updates };
      await this.store.writeAll(allTokens);

      await this.reload();
      return { success: true, message: "Token更新成功" };
    } catch (error) {
      log.error("[GeminiCLI] 启用Token失败:", error.message);
      return { success: false, message: `启用失败: ${error.message}` };
    }
  }

  /**
   * 根据 tokenId 删除 token
   * @param {string} tokenId - 安全的 token ID
   * @returns {Promise<Object>} 操作结果
   */
  async deleteTokenById(tokenId) {
    try {
      const allTokens = await this.store.readAll();
      const salt = await this.store.getSalt();

      const filteredTokens = allTokens.filter(
        (token) => generateTokenId(token.refresh_token, salt) !== tokenId,
      );

      if (filteredTokens.length === allTokens.length) {
        return { success: false, message: "Token不存在" };
      }

      await this.store.writeAll(filteredTokens);

      await this.reload();
      return { success: true, message: "Token删除成功" };
    } catch (error) {
      log.error("[GeminiCLI] 删除Token失败:", error.message);
      return { success: false, message: error.message };
    }
  }

  /**
   * 根据 tokenId 刷新 token
   * @param {string} tokenId - 安全的 token ID
   * @returns {Promise<Object>} 刷新后的 token 信息（不含敏感数据）
   */
  async refreshTokenById(tokenId) {
    const tokenData = await this.findTokenById(tokenId);
    if (!tokenData) {
      throw new TokenError("Token不存在", null, 404);
    }

    const refreshedToken = await this.refreshToken(tokenData);
    return {
      expires_in: refreshedToken.expires_in,
      timestamp: refreshedToken.timestamp,
    };
  }

  /**
   * 获取盐值
   * @returns {Promise<string>} 盐值
   */
  async getSalt() {
    return this.store.getSalt();
  }

  /**
   * 根据 token 对象生成安全 tokenId
   * @param {Object} token - Token 对象
   * @returns {Promise<string|null>}
   */
  async getTokenId(token) {
    if (!token?.refresh_token) return null;
    try {
      const salt = await this.store.getSalt();
      if (!salt) return null;
      return generateTokenId(token.refresh_token, salt);
    } catch (error) {
      log.error(`[GeminiCLI] 生成tokenId失败: ${error.message}`);
      return null;
    }
  }

  // 获取当前轮询配置
  getRotationConfig() {
    return {
      strategy: this.rotationStrategy,
      requestCount: this.requestCountPerToken,
      currentIndex: this.currentIndex,
      totalTokens: this.tokens.length,
      tokenCounts: Object.fromEntries(this.tokenRequestCounts),
    };
  }
}

Object.assign(GeminiCliTokenManager.prototype, lifecycleMethods, projectMethods, {
  RotationStrategy,
  loadRotationConfig,
  updateRotationConfig,
  _hasQuotaForModel,
  _isTokenAvailableForModel,
  _canUseTokenForModel,
  _getTokenModelAvailability,
  _orderTokenCandidates,
  _getDefaultStrategyStartIndex,
  _getOrderedCandidateIndices,
  getRotationProgress,
  randomizeRotationStart,
  getToken,
  _checkAllTokensExhaustedForModel,
  _getTokenForDefaultStrategy,
});

// 导出策略枚举
export { RotationStrategy };

const geminicliTokenManager = new GeminiCliTokenManager();
export default geminicliTokenManager;
