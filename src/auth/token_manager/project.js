import axios from "axios";
import config from "../../config/config.js";
import { TokenError } from "../../utils/errors.js";
import { buildAxiosRequestConfig } from "../../utils/httpClient.js";
import { log } from "../../utils/logger.js";

const DEFAULT_SUBSCRIPTION = "free-tier";

function normalizeSubscription(rawTier) {
  if (!rawTier || typeof rawTier !== "string") {
    return DEFAULT_SUBSCRIPTION;
  }
  return rawTier;
}

function normalizeCredits(rawCredits) {
  if (rawCredits === undefined || rawCredits === null || rawCredits === "") {
    return null;
  }
  const parsed =
    typeof rawCredits === "number" ? rawCredits : Number.parseFloat(rawCredits);
  return Number.isFinite(parsed) ? parsed : null;
}

function resolveProjectIdValue(projectValue) {
  if (!projectValue) return null;
  if (typeof projectValue === "string") return projectValue;
  if (typeof projectValue === "object") return projectValue.id || null;
  return null;
}

function buildSubscriptionSnapshot(data = {}) {
  const sub = normalizeSubscription(data?.paidTier?.id || data?.currentTier?.id);
  const credits = normalizeCredits(data?.paidTier?.availableCredits?.[0]?.creditAmount);

  if (credits !== null) {
    return { sub, credits };
  }

  if (!data?.paidTier || sub === DEFAULT_SUBSCRIPTION) {
    return { sub, credits: 0 };
  }

  return { sub, credits: null };
}

export async function fetchProjectId(token) {
  let loadResult = null;

  try {
    loadResult = (await this._tryLoadCodeAssist(token)) || null;
    if (loadResult?.projectId) return loadResult;
    log.warn(
      "[fetchProjectId] loadCodeAssist 未返回 projectId，回退到 onboardUser",
    );
  } catch (err) {
    log.warn(
      `[fetchProjectId] loadCodeAssist 失败: ${err.message}，回退到 onboardUser`,
    );
  }

  try {
    const onboardResult =
      (await this._tryOnboardUser(token, loadResult?.loadData)) || null;
    if (onboardResult?.projectId) {
      return {
        projectId: onboardResult.projectId,
        sub: onboardResult.sub ?? loadResult?.sub ?? DEFAULT_SUBSCRIPTION,
        credits: onboardResult.credits ?? loadResult?.credits ?? 0,
      };
    }
    log.error("[fetchProjectId] loadCodeAssist 和 onboardUser 均未能获取 projectId");
    return {
      projectId: undefined,
      sub: loadResult?.sub ?? DEFAULT_SUBSCRIPTION,
      credits: loadResult?.credits ?? 0,
    };
  } catch (err) {
    log.error(`[fetchProjectId] onboardUser 失败: ${err.message}`);
    return {
      projectId: undefined,
      sub: loadResult?.sub ?? DEFAULT_SUBSCRIPTION,
      credits: loadResult?.credits ?? 0,
    };
  }
}

export async function fetchSubscriptionAndCredits(token) {
  const data = await this._loadCodeAssistResponse(token);
  return buildSubscriptionSnapshot(data);
}

export async function _loadCodeAssistResponse(token) {
  const apiHost = config.api.host;
  const requestUrl = `https://${apiHost}/v1internal:loadCodeAssist`;
  const requestBody = {
    metadata: {
      ideType: "ANTIGRAVITY",
      platform: "PLATFORM_UNSPECIFIED",
      pluginType: "GEMINI",
    },
  };

  log.info(`[loadCodeAssist] 请求: ${requestUrl}`);
  const response = await axios(
    buildAxiosRequestConfig({
      method: "POST",
      url: requestUrl,
      headers: {
        Host: apiHost,
        "User-Agent": config.api.userAgent,
        Authorization: `Bearer ${token.access_token}`,
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip",
      },
      data: JSON.stringify(requestBody),
      timeout: 30000,
    }),
  );

  return response.data || {};
}

export async function _tryLoadCodeAssist(token) {
  const data = await this._loadCodeAssistResponse(token);
  const { sub, credits } = buildSubscriptionSnapshot(data);
  const projectId = resolveProjectIdValue(data?.cloudaicompanionProject);

  if (data?.currentTier || data?.paidTier || projectId) {
    log.info(`[loadCodeAssist] 订阅信息: sub=${sub}, credits=${credits ?? "null"}`);
  }

  if (data?.currentTier || projectId) {
    log.info("[loadCodeAssist] 用户已激活");
    if (projectId) {
      log.info(`[loadCodeAssist] 成功获取 projectId: ${projectId}`);
      return { projectId, sub, credits, loadData: data };
    }
    log.warn("[loadCodeAssist] 响应中无 projectId");
    return { projectId: null, sub, credits, loadData: data };
  }

  log.info("[loadCodeAssist] 用户未激活 (无 currentTier)");
  return { projectId: null, sub, credits, loadData: data };
}

export async function _tryOnboardUser(token, loadCodeAssistData = null) {
  const apiHost = config.api.host;
  const requestUrl = `https://${apiHost}/v1internal:onboardUser`;

  const tierId = await this._getOnboardTier(token, loadCodeAssistData);
  if (!tierId) {
    log.error("[onboardUser] 无法确定用户 tier");
    return null;
  }

  log.info(`[onboardUser] 用户 tier: ${tierId}`);

  const requestBody = {
    tierId,
    metadata: {
      ideType: "ANTIGRAVITY",
      platform: "PLATFORM_UNSPECIFIED",
      pluginType: "GEMINI",
    },
  };

  log.info(`[onboardUser] 请求: ${requestUrl}`);

  const maxAttempts = 5;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    log.info(`[onboardUser] 轮询尝试 ${attempt}/${maxAttempts}`);

    const response = await axios(
      buildAxiosRequestConfig({
        method: "POST",
        url: requestUrl,
        headers: {
          Host: apiHost,
          "User-Agent": config.api.userAgent,
          Authorization: `Bearer ${token.access_token}`,
          "Content-Type": "application/json",
          "Accept-Encoding": "gzip",
        },
        data: JSON.stringify(requestBody),
        timeout: 30000,
      }),
    );

    const data = response.data;
    const responseData = data?.response || {};
    const priorSnapshot = buildSubscriptionSnapshot(loadCodeAssistData || {});
    const responseSnapshot = buildSubscriptionSnapshot(responseData);
    const sub =
      responseSnapshot.sub !== DEFAULT_SUBSCRIPTION || !loadCodeAssistData
        ? responseSnapshot.sub
        : priorSnapshot.sub || "g1-pro-tier";
    const credits =
      responseSnapshot.credits !== null
        ? responseSnapshot.credits
        : priorSnapshot.credits;

    if (data?.done) {
      log.info("[onboardUser] 操作完成");
      const projectId = resolveProjectIdValue(responseData.cloudaicompanionProject);

      if (projectId) {
        log.info(`[onboardUser] 成功获取 projectId: ${projectId}`);
        return { projectId, sub, credits };
      }
      log.warn("[onboardUser] 操作完成但响应中无 projectId");
      return { projectId: null, sub, credits };
    }

    log.info("[onboardUser] 操作进行中，等待 2 秒...");
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }

  log.error("[onboardUser] 超时：操作未在 10 秒内完成");
  return null;
}

export async function _getOnboardTier(token, loadCodeAssistData = null) {
  log.info(`[_getOnboardTier] 读取默认 tier`);

  try {
    const data = loadCodeAssistData || (await this._loadCodeAssistResponse(token));
    const allowedTiers = data?.allowedTiers || [];
    for (const tier of allowedTiers) {
      if (tier.isDefault) {
        log.info(`[_getOnboardTier] 找到默认 tier: ${tier.id}`);
        return tier.id;
      }
    }

    log.warn("[_getOnboardTier] 未找到默认 tier，使用 LEGACY");
    return "LEGACY";
  } catch (err) {
    log.error(`[_getOnboardTier] 获取 tier 失败: ${err.message}`);
    return null;
  }
}

export async function fetchProjectIdForToken(tokenId) {
  const tokenData = await this.findTokenById(tokenId);
  if (!tokenData) {
    throw new TokenError("Token不存在", null, 404);
  }

  if (this.isExpired(tokenData)) {
    await this.refreshToken(tokenData);
  }

  const { projectId, sub, credits } = (await this.fetchProjectId(tokenData)) || {};

  if (sub !== undefined) {
    tokenData.sub = sub;
  }
  if (credits !== undefined) {
    tokenData.credits = credits;
  }
  this.saveToFile(tokenData);

  const memoryToken = this.tokens.find(
    (token) => token.refresh_token === tokenData.refresh_token,
  );
  if (memoryToken) {
    memoryToken.sub = tokenData.sub;
    memoryToken.credits = tokenData.credits;
  }

  if (!projectId) {
    throw new TokenError("无法获取 projectId，该账号可能无资格", null, 400);
  }

  tokenData.projectId = projectId;
  tokenData.hasQuota = true;
  this.saveToFile(tokenData);

  if (memoryToken) {
    memoryToken.projectId = projectId;
    memoryToken.hasQuota = true;
  }

  return { projectId, sub: tokenData.sub, credits: tokenData.credits };
}
