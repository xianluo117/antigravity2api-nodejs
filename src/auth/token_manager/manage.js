import axios from "axios";
import config from "../../config/config.js";
import { TOKEN_REFRESH_BUFFER } from "../../constants/index.js";
import { OAUTH_CONFIG } from "../../constants/oauth.js";
import { TokenError } from "../../utils/errors.js";
import { buildAxiosRequestConfig, httpRequest } from "../../utils/httpClient.js";
import {
  generateProjectId,
  generateRequestId,
  generateSessionId,
  generateTokenId,
} from "../../utils/idGenerator.js";
import { log } from "../../utils/logger.js";
import quotaManager from "../quota_manager.js";

export async function _refreshExpiredTokensConcurrently() {
  const expiredTokens = this.tokens.filter((token) => this.isExpired(token));
  if (expiredTokens.length === 0) {
    return;
  }

  const salt = await this.store.getSalt();
  const tokenIds = expiredTokens.map((token) =>
    generateTokenId(token.refresh_token, salt),
  );

  log.info(`正在批量刷新 ${tokenIds.length} 个token: ${tokenIds.join(", ")}`);
  const startTime = Date.now();

  const results = await Promise.allSettled(
    expiredTokens.map((token) => this._refreshTokenSafe(token)),
  );

  let successCount = 0;
  let failCount = 0;
  const tokensToDisable = [];
  const disableReasons = [];
  const failedTokenIds = [];

  results.forEach((result, index) => {
    const token = expiredTokens[index];
    const tokenId = tokenIds[index];
    if (result.status === "fulfilled") {
      if (result.value.action === "success") {
        successCount++;
      } else if (result.value.action === "disable") {
        tokensToDisable.push(token);
        disableReasons.push(result.value.reason || "Token刷新失败");
        failCount++;
        failedTokenIds.push(tokenId);
      } else if (result.value.action === "skip") {
        failCount++;
        failedTokenIds.push(tokenId);
      }
    } else {
      failCount++;
      failedTokenIds.push(tokenId);
    }
  });

  for (let i = 0; i < tokensToDisable.length; i++) {
    this.disableToken(tokensToDisable[i], disableReasons[i]);
  }

  const elapsed = Date.now() - startTime;
  if (failCount > 0) {
    log.warn(
      `刷新完成: 成功 ${successCount}, 失败 ${failCount} (${failedTokenIds.join(", ")}), 耗时 ${elapsed}ms`,
    );
  } else {
    log.info(`刷新完成: 成功 ${successCount}, 耗时 ${elapsed}ms`);
  }
}

export async function _refreshTokenSafe(token) {
  try {
    await this.refreshToken(token, true);
    this._clearTokenError(token);
    return { action: "success" };
  } catch (error) {
    const statusCode =
      error.statusCode || error.response?.status || error.status || 500;
    const rawMessage = error.message || "未知错误";
    const reason = `启动检测刷新失败(${statusCode}): ${rawMessage}`;
    this._recordTokenError(token, reason, "startup_refresh");
    if (statusCode === 403 || statusCode === 400) {
      return { action: "disable", reason };
    }
    return { action: "skip", reason };
  }
}

export function _recordTokenError(token, message, stage = "startup_refresh") {
  if (!token) return;
  const trimmed = String(message || "未知错误").slice(0, 800);
  token.lastError = trimmed;
  token.lastErrorTime = Date.now();
  token.lastErrorStage = stage;
  this.saveToFile(token);
}

export function _clearTokenError(token) {
  if (!token) return;
  if (token.lastError || token.lastErrorTime || token.lastErrorStage) {
    token.lastError = null;
    token.lastErrorTime = null;
    token.lastErrorStage = null;
    this.saveToFile(token);
  }
}

export async function _ensureInitialized() {
  if (!this._initPromise) {
    this._initPromise = this._initialize();
  }
  return this._initPromise;
}

export function isExpired(token) {
  if (!token.timestamp || !token.expires_in) return true;
  const expiresAt = token.timestamp + token.expires_in * 1000;
  return Date.now() >= expiresAt - TOKEN_REFRESH_BUFFER;
}

export async function refreshToken(token, silent = false) {
  const salt = await this.store.getSalt();
  const tokenId = generateTokenId(token.refresh_token, salt);
  if (!silent) {
    log.info(`正在刷新token: ${tokenId}`);
  }

  const body = new URLSearchParams({
    client_id: OAUTH_CONFIG.CLIENT_ID,
    client_secret: OAUTH_CONFIG.CLIENT_SECRET,
    grant_type: "refresh_token",
    refresh_token: token.refresh_token,
  });

  try {
    const response = await axios(
      buildAxiosRequestConfig({
        method: "POST",
        url: OAUTH_CONFIG.TOKEN_URL,
        headers: {
          Host: "oauth2.googleapis.com",
          "User-Agent": "Go-http-client/1.1",
          "Content-Type": "application/x-www-form-urlencoded",
          "Accept-Encoding": "gzip",
        },
        data: body.toString(),
      }),
    );

    token.access_token = response.data.access_token;
    token.expires_in = response.data.expires_in;
    token.timestamp = Date.now();

    try {
      const subscription = await this.fetchSubscriptionAndCredits(token);
      if (subscription?.sub !== undefined) {
        token.sub = subscription.sub;
      }
      if (subscription?.credits !== undefined) {
        token.credits = subscription.credits;
      }
    } catch (subscriptionError) {
      log.warn(`刷新后同步订阅/积分失败: ${subscriptionError.message}`);
    }

    this.saveToFile(token);
    return token;
  } catch (error) {
    const statusCode =
      error.response?.status || error.status || error.statusCode || 500;
    const rawBody = error.response?.data;
    const message =
      typeof rawBody === "string"
        ? rawBody
        : rawBody?.error?.message || error.message || "刷新 token 失败";
    const reason = `启动检测刷新失败(${statusCode}): ${message}`;
    this._recordTokenError(token, reason, "startup_refresh");
    throw new TokenError(message, tokenId, statusCode || 500);
  }
}

export function saveToFile(tokenToUpdate = null) {
  this.store.mergeActiveTokens(this.tokens, tokenToUpdate).catch((error) => {
    log.error("保存账号配置文件失败:", error.message);
  });
}

export function disableToken(token, reason = "未知原因") {
  log.warn(`禁用token ...${token.access_token.slice(-8)}, 原因: ${reason}`);
  token.enable = false;
  token.disableReason = reason;
  token.disableTime = Date.now();
  token.lastError = reason;
  token.lastErrorTime = token.disableTime;
  token.lastErrorStage = "disable";
  this.saveToFile();
  this.tokenRequestCounts.delete(token.refresh_token);
  this.tokens = this.tokens.filter(
    (item) => item.refresh_token !== token.refresh_token,
  );
  this.currentIndex = this.currentIndex % Math.max(this.tokens.length, 1);
  this._rebuildAvailableQuotaTokens();
}

export function incrementRequestCount(tokenKey) {
  const current = this.tokenRequestCounts.get(tokenKey) || 0;
  const newCount = current + 1;
  this.tokenRequestCounts.set(tokenKey, newCount);
  return newCount;
}

export function resetRequestCount(tokenKey) {
  this.tokenRequestCounts.set(tokenKey, 0);
}

export function markQuotaExhausted(token) {
  token.hasQuota = false;
  this.saveToFile(token);
  log.warn(`...${token.access_token.slice(-8)}: 额度已耗尽，标记为无额度`);

  if (this.rotationStrategy === "quota_exhausted") {
    const tokenIndex = this.tokens.findIndex(
      (item) => item.refresh_token === token.refresh_token,
    );
    if (tokenIndex !== -1) {
      this._removeQuotaIndex(tokenIndex);
    }
    this.currentIndex = (this.currentIndex + 1) % Math.max(this.tokens.length, 1);
  }
}

export function restoreQuota(token) {
  token.hasQuota = true;
  this.saveToFile(token);
  log.info(`...${token.access_token.slice(-8)}: 额度已恢复`);
}

export async function recordRequest(token, modelId) {
  if (!token || !modelId) return;

  try {
    if (token.refresh_token) {
      this.incrementRequestCount(token.refresh_token);
    }
    const salt = await this.store.getSalt();
    const tokenId = generateTokenId(token.refresh_token, salt);
    quotaManager.recordRequest(tokenId, modelId);
  } catch (error) {
    log.warn("记录请求次数失败:", error.message);
  }
}

export async function _prepareToken(token) {
  if (this.isExpired(token)) {
    await this.refreshToken(token);
  }

  if (!token.projectId) {
    if (config.skipProjectIdFetch) {
      token.projectId = generateProjectId();
      this.saveToFile(token);
      log.info(
        `...${token.access_token.slice(-8)}: 使用随机生成的projectId: ${token.projectId}`,
      );
    } else {
      const { projectId, sub, credits } = (await this.fetchProjectId(token)) || {};
      if (projectId === undefined) {
        log.warn(`...${token.access_token.slice(-8)}: 无资格获取projectId，禁用账号`);
        return {
          action: "disable",
          reason: "无资格获取projectId，该账号可能不支持 Code Assist",
        };
      }
      token.projectId = projectId;
      token.sub = sub;
      token.credits = credits;
      this.saveToFile(token);
    }
  }

  return { action: "ready" };
}

export function _handleTokenError(error, token) {
  const suffix = token.access_token?.slice(-8) || "unknown";
  if (error.statusCode === 403 || error.statusCode === 400) {
    log.warn(`...${suffix}: Token 已失效或错误，已自动禁用该账号`);
    return {
      action: "disable",
      reason: `Token准备失败(${error.statusCode}): ${error.message}`,
    };
  }
  log.error(`...${suffix} 操作失败:`, error.message);
  return { action: "skip" };
}

export function disableCurrentToken(token, reason = "API请求返回403，账号无使用权限") {
  const found = this.tokens.find((item) => item.access_token === token.access_token);
  if (found) {
    this.disableToken(found, reason);
  }
}

export async function reload() {
  this._initPromise = this._initialize();
  await this._initPromise;
  log.info("Token已热重载");
}

export async function addToken(tokenData) {
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

    if (tokenData.projectId) {
      newToken.projectId = tokenData.projectId;
    }
    if (tokenData.email) {
      newToken.email = tokenData.email;
    }
    if (tokenData.hasQuota !== undefined) {
      newToken.hasQuota = tokenData.hasQuota;
    }
    if (tokenData.sub) {
      newToken.sub = tokenData.sub;
    }
    if (tokenData.credits !== undefined) {
      newToken.credits = tokenData.credits;
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
    log.error("添加Token失败:", error.message);
    return { success: false, message: error.message };
  }
}

export async function updateToken(refreshToken, updates) {
  try {
    const allTokens = await this.store.readAll();

    const index = allTokens.findIndex(
      (token) => token.refresh_token === refreshToken,
    );
    if (index === -1) {
      return { success: false, message: "Token不存在" };
    }

    if (updates.enable === true) {
      updates.disableReason = null;
      updates.disableTime = null;
    }
    allTokens[index] = { ...allTokens[index], ...updates };
    await this.store.writeAll(allTokens);

    await this.reload();
    return { success: true, message: "Token更新成功" };
  } catch (error) {
    log.error("更新Token失败:", error.message);
    return { success: false, message: error.message };
  }
}

export async function deleteToken(refreshToken) {
  try {
    const allTokens = await this.store.readAll();

    const filteredTokens = allTokens.filter(
      (token) => token.refresh_token !== refreshToken,
    );
    if (filteredTokens.length === allTokens.length) {
      return { success: false, message: "Token不存在" };
    }

    await this.store.writeAll(filteredTokens);

    await this.reload();
    return { success: true, message: "Token删除成功" };
  } catch (error) {
    log.error("删除Token失败:", error.message);
    return { success: false, message: error.message };
  }
}

export async function getTokenList() {
  try {
    const allTokens = await this.store.readAll();
    const salt = await this.store.getSalt();

    return allTokens.map((token) => ({
      id: generateTokenId(token.refresh_token, salt),
      expires_in: token.expires_in,
      timestamp: token.timestamp,
      enable: token.enable !== false,
      projectId: token.projectId || null,
      email: token.email || null,
      hasQuota: token.hasQuota !== false,
      sub: token.sub || "free-tier",
      credits:
        token.credits ?? ((token.sub || "free-tier") === "free-tier" ? 0 : null),
      disableReason: token.disableReason || null,
      disableTime: token.disableTime || null,
      lastError: token.lastError || null,
      lastErrorTime: token.lastErrorTime || null,
      lastErrorStage: token.lastErrorStage || null,
    }));
  } catch (error) {
    log.error("获取Token列表失败:", error.message);
    return [];
  }
}

export async function findTokenById(tokenId) {
  try {
    const allTokens = await this.store.readAll();
    const salt = await this.store.getSalt();

    return (
      allTokens.find(
        (token) => generateTokenId(token.refresh_token, salt) === tokenId,
      ) || null
    );
  } catch (error) {
    log.error("查找Token失败:", error.message);
    return null;
  }
}

export async function updateTokenById(tokenId, updates) {
  try {
    const allTokens = await this.store.readAll();
    const salt = await this.store.getSalt();

    const index = allTokens.findIndex(
      (token) => generateTokenId(token.refresh_token, salt) === tokenId,
    );

    if (index === -1) {
      return { success: false, message: "Token不存在" };
    }

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
    log.error("更新Token失败:", error.message);
    return { success: false, message: error.message };
  }
}

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

export async function deleteTokenById(tokenId) {
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
    log.error("删除Token失败:", error.message);
    return { success: false, message: error.message };
  }
}

export async function refreshTokenById(tokenId) {
  const tokenData = await this.findTokenById(tokenId);
  if (!tokenData) {
    throw new TokenError("Token不存在", null, 404);
  }

  const refreshedToken = await this.refreshToken(tokenData);
  return {
    expires_in: refreshedToken.expires_in,
    timestamp: refreshedToken.timestamp,
    projectId: refreshedToken.projectId || null,
    sub: refreshedToken.sub || "free-tier",
    credits:
      refreshedToken.credits ??
      ((refreshedToken.sub || "free-tier") === "free-tier" ? 0 : null),
  };
}

export async function getSalt() {
  return this.store.getSalt();
}

export async function getTokenId(token) {
  if (!token?.refresh_token) return null;
  try {
    const salt = await this.store.getSalt();
    if (!salt) return null;
    return generateTokenId(token.refresh_token, salt);
  } catch (error) {
    log.error(`生成tokenId失败: ${error.message}`);
    return null;
  }
}

export function getRotationConfig() {
  return {
    strategy: this.rotationStrategy,
    requestCount: this.requestCountPerToken,
    currentIndex: this.currentIndex,
    totalTokens: this.tokens.length,
    tokenCounts: Object.fromEntries(this.tokenRequestCounts),
  };
}
