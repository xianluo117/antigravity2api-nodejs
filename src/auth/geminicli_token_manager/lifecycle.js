import axios from "axios";
import { TOKEN_REFRESH_BUFFER } from "../../constants/index.js";
import { GEMINICLI_OAUTH_CONFIG } from "../../constants/oauth.js";
import { TokenError } from "../../utils/errors.js";
import { buildAxiosRequestConfig } from "../../utils/httpClient.js";
import { generateTokenId } from "../../utils/idGenerator.js";
import { log } from "../../utils/logger.js";

const PERMANENT_REFRESH_ERROR_TEXTS = [
  "invalid_grant",
  "refresh_token_expired",
  "invalid_refresh_token",
  "unauthorized_client",
  "access_denied",
];

export async function _refreshExpiredTokensConcurrently() {
  const expiredTokens = this.tokens.filter((token) => this.isExpired(token));
  if (expiredTokens.length === 0) {
    return;
  }

  const salt = await this.store.getSalt();
  const tokenIds = expiredTokens.map((token) =>
    generateTokenId(token.refresh_token, salt),
  );

  log.info(
    `[GeminiCLI] 正在批量刷新 ${tokenIds.length} 个token: ${tokenIds.join(", ")}`,
  );
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
      `[GeminiCLI] 刷新完成: 成功 ${successCount}, 失败 ${failCount} (${failedTokenIds.join(", ")}), 耗时 ${elapsed}ms`,
    );
  } else {
    log.info(`[GeminiCLI] 刷新完成: 成功 ${successCount}, 耗时 ${elapsed}ms`);
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

    const isPermanent =
      statusCode === 400 ||
      statusCode === 401 ||
      statusCode === 403 ||
      PERMANENT_REFRESH_ERROR_TEXTS.some((text) =>
        rawMessage.toLowerCase().includes(text),
      );
    if (isPermanent) {
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
    log.info(`[GeminiCLI] 正在刷新token: ${tokenId}`);
  }

  const body = new URLSearchParams({
    client_id: GEMINICLI_OAUTH_CONFIG.CLIENT_ID,
    client_secret: GEMINICLI_OAUTH_CONFIG.CLIENT_SECRET,
    grant_type: "refresh_token",
    refresh_token: token.refresh_token,
  });

  try {
    const response = await axios(
      buildAxiosRequestConfig({
        method: "POST",
        url: GEMINICLI_OAUTH_CONFIG.TOKEN_URL,
        headers: {
          Host: "oauth2.googleapis.com",
          "User-Agent": "google-oauth-playground",
          "Content-Type": "application/x-www-form-urlencoded",
          "Accept-Encoding": "gzip",
        },
        data: body.toString(),
      }),
    );

    token.access_token = response.data.access_token;
    token.expires_in = response.data.expires_in;
    token.timestamp = Date.now();
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

export async function _prepareToken(token) {
  if (this.isExpired(token)) {
    await this.refreshToken(token);
  }

  if (!token.projectId) {
    const result = await this.fetchProjectId(token);
    const projectId = result?.projectId;
    const tier = result?.tier;
    if (!projectId) {
      log.warn(`[GeminiCLI] 无法获取 projectId，禁用账号`);
      return {
        action: "disable",
        reason: "无法获取projectId，该账号可能不支持 Gemini CLI",
      };
    }
    token.projectId = projectId;
    if (tier) {
      token.tier = tier;
    }
    this.saveToFile(token);
  }

  return { action: "ready" };
}

export function _handleTokenError(error, token) {
  const suffix = token.access_token?.slice(-8) || "unknown";
  if (
    error.statusCode === 403 ||
    error.statusCode === 401 ||
    error.statusCode === 400
  ) {
    log.warn(`[GeminiCLI] ...${suffix}: Token 已失效或错误，已自动禁用该账号`);
    return {
      action: "disable",
      reason: `Token准备失败(${error.statusCode}): ${error.message}`,
    };
  }
  log.error(`[GeminiCLI] ...${suffix} 操作失败:`, error.message);
  return { action: "skip" };
}
