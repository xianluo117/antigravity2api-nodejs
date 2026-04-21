import config from "../../config/config.js";
import { httpRequest } from "../../utils/httpClient.js";
import {
  generateRequestId,
  generateSessionId,
  generateTokenId,
} from "../../utils/idGenerator.js";
import { log } from "../../utils/logger.js";

export {
  _ensureInitialized,
  _refreshExpiredTokensConcurrently,
  _refreshTokenSafe,
  _recordTokenError,
  _clearTokenError,
  isExpired,
  refreshToken,
  _prepareToken,
  _handleTokenError,
} from "./lifecycle.js";

export {
  saveToFile,
  disableToken,
  incrementRequestCount,
  resetRequestCount,
  markQuotaExhausted,
  restoreQuota,
  recordRequest,
  disableCurrentToken,
  reload,
  addToken,
  updateToken,
  deleteToken,
  getTokenList,
  findTokenById,
  updateTokenById,
  deleteTokenById,
  refreshTokenById,
  getSalt,
  getTokenId,
  getRotationConfig,
} from "./pool.js";

export async function _sendTestMessage(token) {
  const apiHost = config.api.host;
  const noStreamUrl = config.api.noStreamUrl;

  const sendRequest = async () => {
    const testRequestBody = {
      project: token.projectId,
      requestId: generateRequestId(),
      request: {
        contents: [{ role: "user", parts: [{ text: "hi" }] }],
        generationConfig: {
          maxOutputTokens: 1,
          candidateCount: 1,
        },
        sessionId: generateSessionId(),
      },
      model: "gemini-2.5-flash",
      userAgent: "antigravity",
      requestType: "agent",
    };

    return httpRequest({
      method: "POST",
      url: noStreamUrl,
      headers: {
        Host: apiHost,
        "User-Agent": config.api.userAgent,
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
      if (result?.sub !== undefined) {
        token.sub = result.sub;
      }
      if (result?.credits !== undefined) {
        token.credits = result.credits;
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
        log.info("[Antigravity] 启动检测遇到 projectId 缺失，尝试自动获取后重试");
        const result = await this.fetchProjectId(token);
        if (result?.projectId) {
          token.projectId = result.projectId;
        }
        if (result?.sub !== undefined) {
          token.sub = result.sub;
        }
        if (result?.credits !== undefined) {
          token.credits = result.credits;
        }
        if (token.projectId) {
          await sendRequest();
          return { ok: true };
        }
      } catch (retryError) {
        log.warn(
          `[Antigravity] 自动获取 projectId 后重试失败: ${retryError.message}`,
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

    return { ok: true };
  }
}

export async function enableTokenById(tokenId, options = {}) {
  try {
    const tokenData = await this.findTokenById(tokenId);
    const errorStage = options.stage || "enable_test";

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
      } catch (error) {
        log.error(`[启用检测] 保存错误信息失败: ${error.message}`);
      }
    };

    if (!tokenData) {
      return { success: false, message: "Token不存在" };
    }

    if (tokenData.enable !== false) {
      return { success: true, message: "Token已处于启用状态" };
    }

    log.info(`[启用检测] 正在测试 token ${tokenId} 的可用性...`);

    try {
      await this.refreshToken(tokenData);
    } catch (error) {
      const statusCode = error.statusCode || 500;
      if (statusCode === 403 || statusCode === 400) {
        log.warn(
          `[启用检测] token ${tokenId} 刷新失败(${statusCode}): ${error.message}`,
        );
        const message = `凭证不可用，刷新失败(${statusCode}): ${error.message}`;
        await saveEnableError(message);
        return {
          success: false,
          message,
        };
      }
      log.warn(`[启用检测] token ${tokenId} 刷新失败: ${error.message}`);
      const refreshMessage = `凭证刷新失败: ${error.message}`;
      await saveEnableError(refreshMessage);
      return { success: false, message: refreshMessage };
    }

    try {
      const { projectId, sub, credits } = (await this.fetchProjectId(tokenData)) || {};
      if (sub !== undefined) {
        tokenData.sub = sub;
      }
      if (credits !== undefined) {
        tokenData.credits = credits;
      }
      if (projectId) {
        tokenData.projectId = projectId;
      } else if (!tokenData.projectId && !config.skipProjectIdFetch) {
        log.warn(`[启用检测] token ${tokenId} 无法获取 projectId`);
        const noProjectMessage =
          "凭证不可用: 无法获取 projectId，该账号可能不支持 Code Assist";
        await saveEnableError(noProjectMessage);
        return {
          success: false,
          message: noProjectMessage,
        };
      }
    } catch (error) {
      const statusCode = error.statusCode || 500;
      if (statusCode === 403 || statusCode === 401) {
        log.warn(
          `[启用检测] token ${tokenId} 权限验证失败(${statusCode}): ${error.message}`,
        );
        const permissionMessage =
          `凭证不可用，权限验证失败(${statusCode}): ${error.message}`;
        await saveEnableError(permissionMessage);
        return {
          success: false,
          message: permissionMessage,
        };
      }
      log.warn(
        `[启用检测] token ${tokenId} 获取 projectId 时出现非致命错误: ${error.message}，继续启用`,
      );
    }

    if (tokenData.projectId) {
      log.info(`[启用检测] 正在发送测试消息验证 API 可用性...`);
      const testResult = await this._sendTestMessage(tokenData);
      if (!testResult.ok) {
        log.warn(
          `[启用检测] token ${tokenId} 测试消息失败(${testResult.status}): ${testResult.message}`,
        );
        const testMessage =
          `凭证不可用，API 测试失败(${testResult.status}): ${testResult.message}`;
        await saveEnableError(testMessage);
        return {
          success: false,
          message: testMessage,
        };
      }
      log.info(`[启用检测] token ${tokenId} 测试消息通过`);
    }

    log.info(`[启用检测] token ${tokenId} 全部检测通过，正在启用...`);

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
    if (tokenData.sub) updates.sub = tokenData.sub;
    if (tokenData.credits !== undefined) updates.credits = tokenData.credits;

    allTokens[index] = { ...allTokens[index], ...updates };
    await this.store.writeAll(allTokens);

    await this.reload();
    return { success: true, message: "Token启用成功" };
  } catch (error) {
    log.error("启用Token失败:", error.message);
    return { success: false, message: `启用失败: ${error.message}` };
  }
}
