import axios from "axios";
import config from "../../config/config.js";
import { TokenError } from "../../utils/errors.js";
import { buildAxiosRequestConfig } from "../../utils/httpClient.js";
import { log } from "../../utils/logger.js";

export async function fetchProjectId(token) {
  try {
    const { projectId, sub } = (await this._tryLoadCodeAssist(token)) || {};
    if (projectId) return { projectId, sub };
    log.warn(
      "[fetchProjectId] loadCodeAssist 未返回 projectId，回退到 onboardUser",
    );
  } catch (err) {
    log.warn(
      `[fetchProjectId] loadCodeAssist 失败: ${err.message}，回退到 onboardUser`,
    );
  }

  try {
    const { projectId, sub } = (await this._tryOnboardUser(token)) || {};
    if (projectId) return { projectId, sub };
    log.error("[fetchProjectId] loadCodeAssist 和 onboardUser 均未能获取 projectId");
    return { projectId: undefined, sub: "free-tier" };
  } catch (err) {
    log.error(`[fetchProjectId] onboardUser 失败: ${err.message}`);
    return { projectId: undefined, sub: "free-tier" };
  }
}

export async function _tryLoadCodeAssist(token) {
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
    }),
  );

  const data = response.data;

  let sub = "free-tier";
  if (data?.currentTier) {
    log.info("[loadCodeAssist] 用户已激活");
    const projectId = data.cloudaicompanionProject;
    if (projectId) {
      log.info(`[loadCodeAssist] 成功获取 projectId: ${projectId}`);
      sub = data.currentTier.id;
      return { projectId, sub };
    }
    log.warn("[loadCodeAssist] 响应中无 projectId");
    return null;
  }

  log.info("[loadCodeAssist] 用户未激活 (无 currentTier)");
  return null;
}

export async function _tryOnboardUser(token) {
  const apiHost = config.api.host;
  const requestUrl = `https://${apiHost}/v1internal:onboardUser`;

  const tierId = await this._getOnboardTier(token);
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

    let sub = "g1-pro-tier";
    if (data?.done) {
      log.info("[onboardUser] 操作完成");
      const responseData = data.response || {};
      const projectObj = responseData.cloudaicompanionProject;

      let projectId = null;
      if (typeof projectObj === "object" && projectObj !== null) {
        projectId = projectObj.id;
      } else if (typeof projectObj === "string") {
        projectId = projectObj;
      }

      if (projectId) {
        log.info(`[onboardUser] 成功获取 projectId: ${projectId}`);
        return { projectId, sub };
      }
      log.warn("[onboardUser] 操作完成但响应中无 projectId");
      return null;
    }

    log.info("[onboardUser] 操作进行中，等待 2 秒...");
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }

  log.error("[onboardUser] 超时：操作未在 10 秒内完成");
  return null;
}

export async function _getOnboardTier(token) {
  const apiHost = config.api.host;
  const requestUrl = `https://${apiHost}/v1internal:loadCodeAssist`;
  const requestBody = {
    metadata: {
      ideType: "ANTIGRAVITY",
      platform: "PLATFORM_UNSPECIFIED",
      pluginType: "GEMINI",
    },
  };

  log.info(`[_getOnboardTier] 请求: ${requestUrl}`);

  try {
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

  const { projectId, sub } = (await this.fetchProjectId(tokenData)) || {};
  if (!projectId) {
    throw new TokenError("无法获取 projectId，该账号可能无资格", null, 400);
  }

  tokenData.projectId = projectId;
  tokenData.sub = sub;
  tokenData.hasQuota = true;
  this.saveToFile(tokenData);

  const memoryToken = this.tokens.find(
    (token) => token.refresh_token === tokenData.refresh_token,
  );
  if (memoryToken) {
    memoryToken.projectId = projectId;
    memoryToken.sub = sub;
    memoryToken.hasQuota = true;
  }

  return { projectId };
}
