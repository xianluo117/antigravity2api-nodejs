import config from "../../config/config.js";
import { TokenError } from "../../utils/errors.js";
import { httpRequest } from "../../utils/httpClient.js";
import { generateTokenId } from "../../utils/idGenerator.js";
import { log } from "../../utils/logger.js";
import {
  getUpstreamErrorMessage,
  getUpstreamErrorStatus,
} from "../../utils/upstreamErrorDetails.js";

const GEMINICLI_API_CONFIG = {
  HOST: "cloudcode-pa.googleapis.com",
  USER_AGENT: "GeminiCLI/0.1.5 (Windows; AMD64)",
  BASE_URL: "https://cloudcode-pa.googleapis.com",
};

const TIER_MAP = {
  "g1-ultra-tier": "ultra",
  "ws-ai-ultra-business-tier": "ultra",
  "g1-pro-tier": "pro",
  "helium-tier": "pro",
  "standard-tier": "pro",
  "free-tier": "free",
};

const GEMINICLI_REQUEST_METADATA = {
  ideType: "ANTIGRAVITY",
  platform: "PLATFORM_UNSPECIFIED",
  pluginType: "GEMINI",
};

const ONBOARD_USER_MAX_ATTEMPTS = 5;
const ONBOARD_USER_POLL_INTERVAL_MS = 2000;
const DEFAULT_ONBOARD_TIER_ID = "LEGACY";

function mapTier(rawTier) {
  if (!rawTier) return "pro";
  const lower = rawTier.toLowerCase();
  return TIER_MAP[lower] || "pro";
}

function getGeminiCliApiConfig() {
  const geminicliConfig = config.geminicli?.api || {};
  return {
    baseUrl: geminicliConfig.baseUrl || GEMINICLI_API_CONFIG.BASE_URL,
    host: geminicliConfig.host || GEMINICLI_API_CONFIG.HOST,
    userAgent: geminicliConfig.userAgent || GEMINICLI_API_CONFIG.USER_AGENT,
  };
}

function buildProjectHeaders(token) {
  const apiConfig = getGeminiCliApiConfig();
  return {
    Host: apiConfig.host,
    "User-Agent": apiConfig.userAgent,
    Authorization: `Bearer ${token.access_token}`,
    "Content-Type": "application/json",
    "Accept-Encoding": "gzip",
  };
}

function extractProjectId(projectValue) {
  if (!projectValue) return null;
  if (typeof projectValue === "string") return projectValue;
  if (typeof projectValue === "object") return projectValue.id || null;
  return null;
}

function getDefaultOnboardTierId(loadCodeAssistData) {
  const allowedTiers = loadCodeAssistData?.allowedTiers || [];
  for (const tier of allowedTiers) {
    if (tier.isDefault) {
      return tier.id;
    }
  }
  return DEFAULT_ONBOARD_TIER_ID;
}

export async function fetchProjectId(token) {
  const salt = await this.store.getSalt();
  const tokenId = generateTokenId(token.refresh_token, salt);
  log.info(`[GeminiCLI] 正在获取 projectId 和 tier: ${tokenId}`);

  const apiConfig = getGeminiCliApiConfig();
  const baseUrl = apiConfig.baseUrl;
  const headers = buildProjectHeaders(token);

  try {
    const loadResponse = await httpRequest({
      method: "POST",
      url: `${baseUrl}/v1internal:loadCodeAssist`,
      headers,
      data: {
        metadata: GEMINICLI_REQUEST_METADATA,
      },
      timeout: 30000,
    });

    const data = loadResponse.data;
    const rawTier = data.paidTier?.id || data.currentTier?.id || null;
    const tier = mapTier(rawTier);
    if (rawTier) {
      log.info(`[GeminiCLI] 检测到 tier: ${rawTier} -> ${tier}`);
    }

    if (data.currentTier || data.cloudaicompanionProject) {
      const projectId = extractProjectId(data.cloudaicompanionProject);
      if (projectId) {
        log.info(`[GeminiCLI] 成功获取 projectId: ${projectId}`);
        return { projectId, tier };
      }
      log.warn("[GeminiCLI] loadCodeAssist 响应中无 projectId");
    }

    log.info("[GeminiCLI] 用户未激活，尝试 onboardUser...");
    const onboardResult = await this._tryOnboardUser(token, headers, baseUrl, data);
    if (onboardResult) {
      return { projectId: onboardResult, tier };
    }

    log.info(
      "[GeminiCLI] onboardUser 未返回 projectId，尝试 Google Cloud 项目列表...",
    );
    const gcpProjectId = await this._tryGoogleCloudProjectList(token);
    if (gcpProjectId) {
      return { projectId: gcpProjectId, tier };
    }

    return { projectId: null, tier };
  } catch (error) {
    const status = getUpstreamErrorStatus(error);
    const errorMessage = getUpstreamErrorMessage(error);
    log.error(`[GeminiCLI] 获取 projectId 失败 (${status}):`, errorMessage);

    if (status === 403 || status === 401) {
      throw new TokenError("Token 无权限获取 projectId", tokenId, status);
    }

    log.info(
      "[GeminiCLI] loadCodeAssist 失败，尝试 Google Cloud 项目列表回退...",
    );
    try {
      const gcpProjectId = await this._tryGoogleCloudProjectList(token);
      if (gcpProjectId) {
        return { projectId: gcpProjectId, tier: null };
      }
    } catch (fallbackError) {
      log.error(
        `[GeminiCLI] Google Cloud 项目列表回退也失败:`,
        getUpstreamErrorMessage(fallbackError),
      );
    }

    throw new TokenError(
      `获取 projectId 失败: ${errorMessage}`,
      tokenId,
      status,
    );
  }
}

export async function _tryOnboardUser(
  token,
  headers,
  baseUrl,
  loadCodeAssistData,
) {
  const url = `${baseUrl}/v1internal:onboardUser`;
  const tierId = getDefaultOnboardTierId(loadCodeAssistData);

  const requestBody = {
    tierId,
    metadata: GEMINICLI_REQUEST_METADATA,
  };

  for (let attempt = 1; attempt <= ONBOARD_USER_MAX_ATTEMPTS; attempt++) {
    log.debug(
      `[GeminiCLI] onboardUser 轮询 ${attempt}/${ONBOARD_USER_MAX_ATTEMPTS}`,
    );

    try {
      const response = await httpRequest({
        method: "POST",
        url,
        headers,
        data: requestBody,
        timeout: 30000,
      });

      const data = response.data;

      if (data.done) {
        const responseData = data.response || {};
        const projectId = extractProjectId(responseData.cloudaicompanionProject);

        if (projectId) {
          log.info(`[GeminiCLI] onboardUser 成功获取 projectId: ${projectId}`);
          return projectId;
        }
        log.warn("[GeminiCLI] onboardUser 完成但响应中无 projectId");
        return null;
      }

      await new Promise((resolve) => setTimeout(resolve, ONBOARD_USER_POLL_INTERVAL_MS));
    } catch (error) {
      log.error(`[GeminiCLI] onboardUser 失败:`, getUpstreamErrorMessage(error));
      throw error;
    }
  }

  log.error("[GeminiCLI] onboardUser 超时");
  return null;
}

export async function _tryGoogleCloudProjectList(token) {
  try {
    const apiConfig = getGeminiCliApiConfig();
    const response = await httpRequest({
      method: "GET",
      url: "https://cloudresourcemanager.googleapis.com/v1/projects",
      headers: {
        Authorization: `Bearer ${token.access_token}`,
        "User-Agent": apiConfig.userAgent,
        "Accept-Encoding": "gzip",
      },
      timeout: 30000,
    });

    const projects = (response.data?.projects || []).filter(
      (project) => project.lifecycleState === "ACTIVE",
    );

    if (projects.length === 0) {
      log.warn("[GeminiCLI] Google Cloud 项目列表为空");
      return null;
    }

    let selected = null;
    if (projects.length === 1) {
      selected = projects[0];
    } else {
      selected = projects.find(
        (project) =>
          (project.projectId &&
            project.projectId.toLowerCase().includes("default")) ||
          (project.name && project.name.toLowerCase().includes("default")),
      );
      if (!selected) {
        selected = projects[0];
      }
    }

    const projectId = selected?.projectId || null;
    if (projectId) {
      log.info(
        `[GeminiCLI] 从 Google Cloud 项目列表选择: ${projectId} (共 ${projects.length} 个活跃项目)`,
      );
      return projectId;
    }

    return null;
  } catch (error) {
    const status = getUpstreamErrorStatus(error);
    log.error(
      `[GeminiCLI] Google Cloud 项目列表查询失败 (${status}):`,
      getUpstreamErrorMessage(error),
    );
    return null;
  }
}

export async function fetchProjectIdForToken(tokenId) {
  const tokenData = await this.findTokenById(tokenId);
  if (!tokenData) {
    throw new TokenError("Token不存在", null, 404);
  }

  return this.fetchProjectIdForTokenData(tokenData, tokenId);
}

export async function fetchProjectIdForTokenData(tokenData, tokenId = null) {
  if (!tokenData) {
    throw new TokenError("Token不存在", null, 404);
  }

  const effectiveTokenId =
    tokenId ||
    (await this.store
      .getSalt()
      .then((salt) => generateTokenId(tokenData.refresh_token, salt)));

  if (this.isExpired(tokenData)) {
    const refreshedToken = await this.refreshToken(tokenData);
    if (refreshedToken) {
      tokenData = refreshedToken;
    }
  }

  const result = await this.fetchProjectId(tokenData);
  const projectId = result?.projectId;
  const tier = result?.tier;
  if (!projectId) {
    throw new TokenError("无法获取 projectId，该账号可能无资格", null, 400);
  }

  tokenData.projectId = projectId;
  if (tier) {
    tokenData.tier = tier;
  }

  const allTokens = await this.store.readAll();
  const salt = await this.store.getSalt();
  const index = allTokens.findIndex(
    (token) => generateTokenId(token.refresh_token, salt) === effectiveTokenId,
  );
  if (index !== -1) {
    allTokens[index].projectId = projectId;
    if (tier) {
      allTokens[index].tier = tier;
    }
    await this.store.writeAll(allTokens);
  }

  const memoryToken = this.tokens.find(
    (token) => token.refresh_token === tokenData.refresh_token,
  );
  if (memoryToken) {
    memoryToken.projectId = projectId;
    if (tier) {
      memoryToken.tier = tier;
    }
  }

  return { projectId, tier, tokenId: effectiveTokenId };
}
