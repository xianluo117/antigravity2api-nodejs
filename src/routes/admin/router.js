import dotenv from "dotenv";
import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import { getModelsWithQuotas } from "../../api/client.js";
import { getGeminiCliQuotas } from "../../api/geminicli_client.js";
import geminicliTokenManager from "../../auth/geminicli_token_manager.js";
import { generateToken } from "../../auth/jwt.js";
import oauthManager from "../../auth/oauth_manager.js";
import quotaManager from "../../auth/quota_manager.js";
import tokenManager from "../../auth/token_manager.js";
import config, { getConfigJson, saveConfigJson } from "../../config/config.js";
import fingerprintRequester from "../../requester.js";
import { reloadConfig } from "../../utils/configReloader.js";
import { deepMerge } from "../../utils/deepMerge.js";
import { parseEnvFile, updateEnvFile } from "../../utils/envParser.js";
import { httpRequest } from "../../utils/httpClient.js";
import ipBlockManager from "../../utils/ipBlockManager.js";
import logger from "../../utils/logger.js";
import memoryManager from "../../utils/memoryManager.js";
import { getEnvPath } from "../../utils/paths.js";
import {
  getUpstreamErrorMessage,
  getUpstreamErrorStatus,
} from "../../utils/upstreamErrorDetails.js";
import {
  getProxyPoolSummary,
  moveProxyToDisabledPool,
  normalizeProxyPoolInput,
  normalizeProxyProtocol,
  parseProxyPool,
} from "../../utils/proxyPool.js";
import {
  COOKIE_OPTIONS,
  buildAntigravityExportData,
  buildBatchExportResults,
  buildBatchResponsePayload,
  buildForbiddenGoogleAccountList,
  buildGeminiCliExportData,
  cookieAuthMiddleware,
  cookieOrPasswordAuthMiddleware,
  deriveExpiresInAndTimestamp,
  extractGeminiCliImportList,
  findFieldByKeyword,
  findTokenByEmail,
  getClientIP,
  getRequestEmail,
  getRequestMode,
  getRequestPassword,
  normalizeBatchTokenIds,
  parseGeminiCliEnable,
  refreshAntigravityQuotaByTokenId,
  refreshGeminiCliQuotaByTokenId,
  smartParseGeminiCliToken,
  smartParseToken,
  verifyPassword,
} from "./shared.js";

const envPath = getEnvPath();
const PROXY_TEST_TARGET_MODEL = "gemini-2.5-flash";
const PROXY_TEST_TIMEOUT = 15000;
const PROXY_TEST_CONCURRENCY = 10;
const PROXY_TEST_MAX_CONCURRENCY = 100;
const ANTIGRAVITY_ROTATION_GROUPS = {
  claude: { label: "Claude", modelId: "claude-sonnet-4-6-thinking" },
  gemini: { label: "Gemini", modelId: "gemini-2.5-flash" },
  banana: { label: "Banana", modelId: "gemini-3.1-flash-image" },
};
const GEMINICLI_ROTATION_GROUPS = {
  pro: { label: "Pro", modelId: "gemini-2.5-pro" },
  flash: { label: "Flash", modelId: "gemini-2.5-flash" },
};
const __dirname = path.dirname(fileURLToPath(import.meta.url));

const router = express.Router();
let proxyTestRequester = null;

function getRotationTokenLabel(tokenId) {
  return tokenId ? tokenId.slice(0, 12) : "unknown-token";
}

async function warmupAntigravityRotationQuotas() {
  const enabledTokens = [...tokenManager.tokens];
  const successfulIndices = [];

  for (const token of enabledTokens) {
    let activeToken = token;
    const tokenId = activeToken.tokenId || (await tokenManager.getTokenId(activeToken));

    if (!tokenId) continue;

    try {
      if (tokenManager.isExpired(activeToken)) {
        activeToken = await tokenManager.refreshToken(activeToken, true);
      }

      if (!activeToken.projectId) {
        await tokenManager.fetchProjectIdForToken(tokenId);
        const refreshedToken = tokenManager.tokens.find(
          (item) =>
            item.tokenId === tokenId ||
            item.refresh_token === activeToken.refresh_token,
        );
        activeToken = refreshedToken || activeToken;
      }

      if (!activeToken.projectId) {
        logger.warn(
          `[Rotation] Antigravity 凭证 ${getRotationTokenLabel(tokenId)} 缺少 projectId，跳过额度初始化`,
        );
        continue;
      }

      const quotas = await getModelsWithQuotas(activeToken);
      quotaManager.updateQuota(tokenId, quotas);

      const tokenIndex = tokenManager.tokens.findIndex(
        (item) =>
          item.tokenId === tokenId ||
          item.refresh_token === activeToken.refresh_token,
      );
      if (tokenIndex >= 0 && Object.keys(quotas || {}).length > 0) {
        successfulIndices.push(tokenIndex);
      }
    } catch (error) {
      logger.warn(
        `[Rotation] 预热 Antigravity 凭证 ${getRotationTokenLabel(tokenId)} 额度失败: ${error.message}`,
      );
    }
  }

  return successfulIndices;
}

async function warmupGeminiCliRotationQuotas() {
  const enabledTokens = [...geminicliTokenManager.tokens];
  const successfulIndices = [];

  for (const token of enabledTokens) {
    let activeToken = token;
    const tokenId =
      activeToken.tokenId || (await geminicliTokenManager.getTokenId(activeToken));

    if (!tokenId) continue;

    try {
      if (geminicliTokenManager.isExpired(activeToken)) {
        activeToken = await geminicliTokenManager.refreshToken(activeToken, true);
      }

      if (!activeToken.projectId) {
        await geminicliTokenManager.fetchProjectIdForTokenData(activeToken, tokenId);
        const refreshedToken = geminicliTokenManager.tokens.find(
          (item) =>
            item.tokenId === tokenId ||
            item.refresh_token === activeToken.refresh_token,
        );
        activeToken = refreshedToken || activeToken;
      }

      if (!activeToken.projectId) {
        logger.warn(
          `[Rotation] Gemini CLI 凭证 ${getRotationTokenLabel(tokenId)} 缺少 projectId，跳过额度初始化`,
        );
        continue;
      }

      const quotas = await getGeminiCliQuotas(activeToken);
      quotaManager.updateQuota(tokenId, quotas, "geminicli");

      const tokenIndex = geminicliTokenManager.tokens.findIndex(
        (item) =>
          item.tokenId === tokenId ||
          item.refresh_token === activeToken.refresh_token,
      );
      if (tokenIndex >= 0 && Object.keys(quotas || {}).length > 0) {
        successfulIndices.push(tokenIndex);
      }
    } catch (error) {
      logger.warn(
        `[Rotation] 预热 Gemini CLI 凭证 ${getRotationTokenLabel(tokenId)} 额度失败: ${error.message}`,
      );
    }
  }

  return successfulIndices;
}

function getProxyTestModeLabel() {
  return config.useNativeAxios === true ? "Axios" : "TLS";
}

function getProxyTestTargetUrl() {
  return config.api?.noStreamUrl || config.api?.url;
}

function buildProxyTestHeaders() {
  return {
    Host: config.api?.host,
    "User-Agent": config.api?.userAgent || "antigravity/proxy-test",
    "Content-Type": "application/json",
    "Accept-Encoding": "gzip",
  };
}

function buildProxyTestRequestBody() {
  return {
    project: "proxy-test",
    requestId: `proxy-test-${Date.now()}`,
    request: {
      contents: [{ role: "user", parts: [{ text: "hi" }] }],
      generationConfig: {
        maxOutputTokens: 1,
        candidateCount: 1,
      },
      sessionId: `proxy-test-session-${Date.now()}`,
    },
    model: PROXY_TEST_TARGET_MODEL,
    userAgent: "antigravity",
    requestType: "agent",
  };
}

function getProxyTestRequester() {
  if (proxyTestRequester) return proxyTestRequester;

  const isPkg = typeof process.pkg !== "undefined";
  const configPath = isPkg
    ? path.join(path.dirname(process.execPath), "bin", "tls_config.json")
    : path.join(__dirname, "..", "..", "bin", "tls_config.json");

  proxyTestRequester = fingerprintRequester.create({
    configPath,
    timeout: Math.ceil(PROXY_TEST_TIMEOUT / 1000),
  });

  return proxyTestRequester;
}

function summarizeProxyTestError(error) {
  if (!error) return "未知错误";
  const upstreamStatus = error?.response?.status;
  if (upstreamStatus) {
    return `HTTP ${upstreamStatus}`;
  }
  return error.message || "请求失败";
}

function inferProxyTestStatus(error) {
  const responseStatus = error?.response?.status;
  if (Number.isInteger(responseStatus)) {
    return responseStatus;
  }

  const candidates = [error?.message, error?.error, error?.response?.data]
    .filter(Boolean)
    .map((value) => String(value));

  for (const text of candidates) {
    const match = text.match(/\b(407|403|401|400|429|500|502|503|504)\b/);
    if (match) {
      return Number.parseInt(match[1], 10);
    }
    if (/proxy authentication required/i.test(text)) {
      return 407;
    }
  }

  return null;
}

function buildProxyTestLogText(payload = {}) {
  const summary = payload.summary || {};
  const results = Array.isArray(payload.results) ? payload.results : [];
  const headerLines = [
    `proxy-test-time: ${new Date().toISOString()}`,
    `request-mode: ${summary.requestMode || "-"}`,
    `target-url: ${summary.targetUrl || "-"}`,
    `concurrency: ${summary.concurrency || 0}`,
    `tested: ${summary.tested || 0}`,
    `success: ${summary.success || 0}`,
    `failed: ${summary.failed || 0}`,
    `auto-disabled: ${summary.autoDisabled || 0}`,
    "",
  ];

  const sections = results.map((item, index) => {
    const lines = Array.isArray(item.logLines) ? item.logLines : [];
    return [
      `===== proxy #${index + 1} =====`,
      `raw: ${item.raw || ""}`,
      `mode: ${item.requestMode || ""}`,
      `status: ${item.status ?? ""}`,
      `success: ${item.success === true}`,
      `autoDisabled: ${item.autoDisabled === true}`,
      `message: ${item.message || ""}`,
      ...lines,
      "",
    ].join("\n");
  });

  return [...headerLines, ...sections].join("\n");
}

function countProxyPoolEntries(poolRaw = "") {
  const normalized = normalizeProxyPoolInput(poolRaw);
  return normalized ? normalized.split("\n").length : 0;
}

function normalizeProxyTestConcurrency(value) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return PROXY_TEST_CONCURRENCY;
  }
  return Math.min(parsed, PROXY_TEST_MAX_CONCURRENCY);
}

async function runWithConcurrency(items, concurrency, worker) {
  const list = Array.isArray(items) ? items : [];
  const limit = Math.max(1, Number(concurrency) || 1);
  const results = new Array(list.length);
  let nextIndex = 0;

  async function runner() {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      if (currentIndex >= list.length) {
        return;
      }
      results[currentIndex] = await worker(list[currentIndex], currentIndex);
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(limit, list.length) }, () => runner()),
  );

  return results;
}

async function testSingleProxyConnectivity({ proxyEntry, proxyProtocol }) {
  const startedAt = Date.now();
  const requestMode = getProxyTestModeLabel();
  const targetUrl = getProxyTestTargetUrl();
  const result = {
    raw: proxyEntry.raw,
    url: proxyEntry.url,
    protocol: proxyEntry.protocol || proxyProtocol,
    host: proxyEntry.host,
    port: proxyEntry.port,
    username: proxyEntry.username,
    password: proxyEntry.password,
    targetUrl,
    requestMode,
    success: false,
    status: null,
    durationMs: 0,
    autoDisabled: false,
    disableResult: null,
    shouldDisable: false,
    message: "",
    logLines: [],
  };

  try {
    const headers = buildProxyTestHeaders();
    const requestBody = buildProxyTestRequestBody();
    const proxyConfig = {
      enabled: true,
      mode: "pool",
      protocol: proxyProtocol,
      poolRaw: proxyEntry.raw,
      disabledPoolRaw: "",
      url: null,
    };
    let response;
    result.logLines.push(
      `[start] mode=${requestMode} target=${targetUrl} proxy=${proxyEntry.url}`,
    );

    if (config.useNativeAxios === true) {
      result.logLines.push("[request] transport=Axios method=POST no-credential");
      response = await httpRequest({
        method: "POST",
        url: targetUrl,
        headers,
        timeout: Math.min(Number(config.timeout) || 300000, PROXY_TEST_TIMEOUT),
        skipProxyAutoDisable: true,
        proxy: proxyConfig,
        data: requestBody,
        responseType: "text",
        validateStatus: () => true,
      });
    } else {
      const requester = getProxyTestRequester();
      result.logLines.push("[request] transport=TLS method=POST no-credential");
      response = await requester.request({
        method: "POST",
        url: targetUrl,
        headers,
        data: requestBody,
        timeout: Math.ceil(
          Math.min(Number(config.timeout) || 300000, PROXY_TEST_TIMEOUT) / 1000,
        ),
        proxy: proxyConfig,
        responseType: "text",
        validateStatus: () => true,
      });
    }

    result.status = response.status;
    result.durationMs = Date.now() - startedAt;
    result.logLines.push(
      `[response] status=${response.status} durationMs=${result.durationMs}`,
    );
    if (typeof response.data === "string" && response.data) {
      result.logLines.push(
        `[response-body] ${response.data.slice(0, 1000).replace(/\r\n/g, "\\n")}`,
      );
    }

    if (response.status === 407) {
      result.success = false;
      result.shouldDisable = true;
      result.message = "代理认证失败(407)，已自动转移到禁用池";
      return result;
    }

    result.success = true;
    result.message = `已通过代理打通 LLM 请求链路，上游返回 ${response.status}，按规则视为通过`;
    return result;
  } catch (error) {
    result.durationMs = Date.now() - startedAt;
    result.status = inferProxyTestStatus(error);
    result.message = summarizeProxyTestError(error);
    result.logLines.push(
      `[error] status=${result.status ?? ""} durationMs=${result.durationMs} message=${result.message}`,
    );
    const errorBody =
      typeof error?.response?.data === "string"
        ? error.response.data
        : error?.response?.data
          ? JSON.stringify(error.response.data)
          : "";
    if (errorBody) {
      result.logLines.push(
        `[error-body] ${errorBody.slice(0, 1000).replace(/\r\n/g, "\\n")}`,
      );
    }

    if (result.status === 407) {
      result.shouldDisable = true;
      result.message = "代理认证失败(407)，已自动转移到禁用池";
    }

    return result;
  }
}

router.use((req, res, next) => {
  res.set(
    "Cache-Control",
    "no-store, no-cache, must-revalidate, proxy-revalidate",
  );
  res.set("Pragma", "no-cache");
  res.set("Expires", "0");
  next();
});

router.post("/login", async (req, res) => {
  const clientIP = getClientIP(req);

  const blockStatus = ipBlockManager.check(clientIP);
  if (blockStatus.blocked) {
    if (blockStatus.reason === "permanent") {
      return res
        .status(403)
        .json({ success: false, message: "您的IP已被永久封禁" });
    }
    const remainingMinutes = Math.ceil(
      (blockStatus.expiresAt - Date.now()) / 60000,
    );
    return res.status(429).json({
      success: false,
      message: `登录尝试过多，请 ${remainingMinutes} 分钟后重试`,
      retryAfter: remainingMinutes * 60,
    });
  }

  const { username, password } = req.body;

  if (
    !username ||
    !password ||
    typeof username !== "string" ||
    typeof password !== "string"
  ) {
    return res
      .status(400)
      .json({ success: false, message: "用户名和密码必填" });
  }

  if (username.length > 100 || password.length > 100) {
    return res.status(400).json({ success: false, message: "输入过长" });
  }

  if (username === config.admin.username && password === config.admin.password) {
    const token = generateToken({ username, role: "admin" });

    res.cookie("authToken", token, {
      ...COOKIE_OPTIONS,
      secure: req.secure || process.env.NODE_ENV === "production",
    });

    logger.info(`管理员登录成功 IP: ${clientIP}`);
    res.json({ success: true, token });
  } else {
    await ipBlockManager.recordViolation(clientIP, "admin_login_fail");
    logger.warn(`管理员登录失败 IP: ${clientIP}`);
    res.status(401).json({ success: false, message: "用户名或密码错误" });
  }
});

router.post("/logout", (req, res) => {
  res.clearCookie("authToken", {
    ...COOKIE_OPTIONS,
    secure: req.secure || process.env.NODE_ENV === "production",
  });
  res.json({ success: true, message: "已登出" });
});

router.get("/tokens", cookieAuthMiddleware, async (req, res) => {
  try {
    const tokens = await tokenManager.getTokenList();
    res.json({ success: true, data: tokens });
  } catch (error) {
    logger.error("获取Token列表失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.get("/token-summary", cookieOrPasswordAuthMiddleware, async (req, res) => {
  try {
    const [antigravityTokens, geminicliTokens] = await Promise.all([
      tokenManager.getTokenList(),
      geminicliTokenManager.getTokenList(),
    ]);

    const buildSummary = (tokens) => ({
      total: tokens.length,
      enabled: tokens.filter((token) => token.enable).length,
      disabled: tokens.filter((token) => !token.enable).length,
    });

    res.json({
      success: true,
      data: {
        antigravity: buildSummary(antigravityTokens),
        geminicli: buildSummary(geminicliTokens),
      },
    });
  } catch (error) {
    logger.error("获取Token统计失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.get("/oauth/403-accounts", async (req, res) => {
  const password = getRequestPassword(req);
  const queryEmail = String(getRequestEmail(req) || "")
    .trim()
    .toLowerCase();
  if (!password || !verifyPassword(password)) {
    return res.status(403).json({ success: false, message: "密码验证失败" });
  }

  try {
    const [antigravityTokens, geminicliTokens] = await Promise.all([
      tokenManager.getTokenList(),
      geminicliTokenManager.getTokenList(),
    ]);

    const antigravityResult = buildForbiddenGoogleAccountList(
      antigravityTokens,
      "antigravity",
    );
    const geminicliResult = buildForbiddenGoogleAccountList(
      geminicliTokens,
      "geminicli",
      antigravityResult.seenEmails,
    );

    const mergedAccounts = [
      ...antigravityResult.items,
      ...geminicliResult.items,
    ];

    if (queryEmail) {
      const matched = mergedAccounts.find(
        (item) =>
          String(item.email || "")
            .trim()
            .toLowerCase() === queryEmail,
      );

      if (!matched) {
        return res.status(404).json({
          success: false,
          message: "未找到该邮箱对应的认证URL",
        });
      }

      return res.json({
        success: true,
        data: {
          email: matched.email,
          url: matched.url,
          enable: matched.enable === true,
        },
      });
    }

    res.json({
      success: true,
      data: {
        accounts: mergedAccounts,
        antigravity: antigravityResult.items,
        geminicli: geminicliResult.items,
      },
    });
  } catch (error) {
    logger.error("获取403账号认证URL列表失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post("/oauth/enable-by-email", async (req, res) => {
  const password = getRequestPassword(req);
  const email = String(getRequestEmail(req) || "").trim();
  const mode = getRequestMode(req, "auto");

  if (!password || !verifyPassword(password)) {
    return res.status(403).json({ success: false, message: "密码验证失败" });
  }

  if (!email) {
    return res.status(400).json({ success: false, message: "email必填" });
  }

  const validModes = new Set(["auto", "antigravity", "geminicli", "cli"]);
  if (!validModes.has(mode)) {
    return res.status(400).json({
      success: false,
      message: "mode仅支持 auto / antigravity / geminicli / cli",
    });
  }

  try {
    const runEnable = async (source, token) => {
      const manager =
        source === "antigravity" ? tokenManager : geminicliTokenManager;
      const result = await manager.enableTokenById(token.id, {
        stage: "manual",
      });

      return {
        source,
        tokenId: token.id,
        success: result.success !== false,
        message: result.message || "操作完成",
      };
    };

    const [antigravityTokens, geminicliTokens] = await Promise.all([
      tokenManager.getTokenList(),
      geminicliTokenManager.getTokenList(),
    ]);

    const antigravityToken =
      mode === "auto" || mode === "antigravity"
        ? findTokenByEmail(antigravityTokens, email)
        : null;
    const geminicliToken =
      mode === "auto" || mode === "geminicli" || mode === "cli"
        ? findTokenByEmail(geminicliTokens, email)
        : null;

    if (!antigravityToken && !geminicliToken) {
      return res.status(404).json({
        success: false,
        message: "未找到该邮箱对应的凭证",
      });
    }

    if (mode === "auto") {
      if (antigravityToken) {
        const antigravityResult = await runEnable("antigravity", antigravityToken);

        if (!antigravityResult.success) {
          return res.json({
            success: false,
            message: antigravityResult.message,
            data: {
              email,
              mode: "antigravity",
              enable: false,
              antigravity: antigravityResult,
              geminicli: null,
            },
          });
        }

        if (geminicliToken) {
          const geminicliResult = await runEnable("geminicli", geminicliToken);
          return res.json({
            success: geminicliResult.success,
            message: geminicliResult.success
              ? "Antigravity 与 CLI 凭证均启动成功"
              : geminicliResult.message,
            data: {
              email,
              mode: geminicliResult.success ? "auto" : "geminicli",
              enable: geminicliResult.success,
              antigravity: antigravityResult,
              geminicli: geminicliResult,
            },
          });
        }

        return res.json({
          success: true,
          message: antigravityResult.message,
          data: {
            email,
            mode: "antigravity",
            enable: true,
            antigravity: antigravityResult,
            geminicli: null,
          },
        });
      }

      const geminicliResult = await runEnable("geminicli", geminicliToken);
      return res.json({
        success: geminicliResult.success,
        message: geminicliResult.message,
        data: {
          email,
          mode: "geminicli",
          enable: geminicliResult.success,
          antigravity: null,
          geminicli: geminicliResult,
        },
      });
    }

    const selected = mode === "antigravity" ? antigravityToken : geminicliToken;
    const selectedSource = mode === "antigravity" ? "antigravity" : "geminicli";
    const result = await runEnable(selectedSource, selected);

    return res.json({
      success: result.success,
      message: result.message,
      data: {
        email,
        mode: selectedSource,
        tokenId: selected.id,
        enable: result.success,
      },
    });
  } catch (error) {
    logger.error("按邮箱启用凭证失败:", error.message);
    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
});

router.post("/tokens", cookieAuthMiddleware, async (req, res) => {
  const {
    access_token,
    refresh_token,
    expires_in,
    timestamp,
    enable,
    projectId,
    email,
    sub,
    credits,
  } = req.body;
  if (!access_token || !refresh_token) {
    return res
      .status(400)
      .json({ success: false, message: "access_token和refresh_token必填" });
  }
  const tokenData = { access_token, refresh_token, expires_in, sub, credits };
  if (timestamp) tokenData.timestamp = timestamp;
  if (enable !== undefined) tokenData.enable = enable;
  if (projectId) tokenData.projectId = projectId;
  if (email) tokenData.email = email;

  try {
    const result = await tokenManager.addToken(tokenData);
    logger.info(`添加新Token: ${access_token.substring(0, 8)}...`);
    res.json(result);
  } catch (error) {
    logger.error("添加Token失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.put("/tokens/:tokenId", cookieAuthMiddleware, async (req, res) => {
  const { tokenId } = req.params;
  const updates = req.body;

  delete updates.access_token;
  delete updates.refresh_token;

  try {
    if (updates.enable === true && updates.enableWithTest) {
      delete updates.enableWithTest;
      const result = await tokenManager.enableTokenById(tokenId);
      if (result.success) {
        logger.info(`启用Token(已验证): ${tokenId}`);
      } else {
        logger.warn(`启用Token验证失败: ${tokenId} - ${result.message}`);
      }
      res.json(result);
    } else {
      const result = await tokenManager.updateTokenById(tokenId, updates);
      logger.info(`更新Token: ${tokenId}`);
      res.json(result);
    }
  } catch (error) {
    const message = getUpstreamErrorMessage(error);
    logger.error("更新Token失败:", message);
    res.status(getUpstreamErrorStatus(error)).json({ success: false, message });
  }
});

router.delete("/tokens/:tokenId", cookieAuthMiddleware, async (req, res) => {
  const { tokenId } = req.params;
  try {
    const result = await tokenManager.deleteTokenById(tokenId);
    logger.info(`删除Token: ${tokenId}`);
    res.json(result);
  } catch (error) {
    logger.error("删除Token失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post("/tokens/reload", cookieAuthMiddleware, async (req, res) => {
  try {
    await tokenManager.reload();
    logger.info("手动触发Token热重载");
    res.json({ success: true, message: "Token已热重载" });
  } catch (error) {
    logger.error("热重载失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post("/tokens/batch", cookieAuthMiddleware, async (req, res) => {
  const action = String(req.body?.action || "").trim();
  const tokenIds = normalizeBatchTokenIds(req.body?.tokenIds);

  if (!action) {
    return res.status(400).json({ success: false, message: "action必填" });
  }

  if (tokenIds.length === 0) {
    return res.status(400).json({ success: false, message: "tokenIds不能为空" });
  }

  try {
    if (action === "export") {
      const { password } = req.body || {};
      if (!password || !verifyPassword(password)) {
        return res.status(403).json({ success: false, message: "密码验证失败" });
      }

      const { results, tokens } = await buildBatchExportResults(tokenIds, (tokenId) =>
        tokenManager.findTokenById(tokenId),
      );
      const payload = buildBatchResponsePayload(action, results, {
        exportData: buildAntigravityExportData(tokens),
      });

      return res.json({
        success: true,
        message: `批量导出完成：成功 ${payload.successCount} 个，失败 ${payload.failCount} 个`,
        data: payload,
      });
    }

    const results = [];

    for (const tokenId of tokenIds) {
      try {
        let result;

        if (action === "enable") {
          result = await tokenManager.enableTokenById(tokenId, { stage: "manual" });
        } else if (action === "disable") {
          const now = Date.now();
          result = await tokenManager.updateTokenById(tokenId, {
            enable: false,
            disableReason: "手动批量禁用",
            disableTime: now,
            lastError: "手动批量禁用",
            lastErrorTime: now,
            lastErrorStage: "manual",
          });
        } else if (action === "fetch_project_id") {
          const projectResult = await tokenManager.fetchProjectIdForToken(tokenId);
          result = {
            success: true,
            message: "Project ID获取成功",
            projectId: projectResult.projectId,
            sub: projectResult.sub,
            credits: projectResult.credits,
          };
        } else if (action === "refresh_token") {
          const refreshResult = await tokenManager.refreshTokenById(tokenId);
          result = {
            success: true,
            message: "Token刷新成功",
            data: refreshResult,
          };
        } else if (action === "refresh_quota") {
          const quotas = await refreshAntigravityQuotaByTokenId(tokenId);
          result = {
            success: true,
            message: "额度刷新成功",
            modelCount: Object.keys(quotas || {}).length,
          };
        } else if (action === "delete") {
          result = await tokenManager.deleteTokenById(tokenId);
        } else {
          return res.status(400).json({
            success: false,
            message: `不支持的批量操作: ${action}`,
          });
        }

        results.push({
          tokenId,
          success: result.success !== false,
          message: result.message || "操作成功",
          ...(result.projectId ? { projectId: result.projectId } : {}),
          ...(result.sub !== undefined ? { sub: result.sub } : {}),
          ...(result.credits !== undefined ? { credits: result.credits } : {}),
          ...(result.data ? { data: result.data } : {}),
          ...(result.modelCount !== undefined ? { modelCount: result.modelCount } : {}),
        });
      } catch (error) {
        results.push({
          tokenId,
          success: false,
          message: getUpstreamErrorMessage(error, "操作失败"),
        });
      }
    }

    const payload = buildBatchResponsePayload(action, results);
    res.json({
      success: true,
      message: `批量操作完成：成功 ${payload.successCount} 个，失败 ${payload.failCount} 个`,
      data: payload,
    });
  } catch (error) {
    logger.error("批量操作Token失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post("/tokens/:tokenId/refresh", cookieAuthMiddleware, async (req, res) => {
  const { tokenId } = req.params;
  try {
    const result = await tokenManager.refreshTokenById(tokenId);
    logger.info(`手动刷新Token: ${tokenId}`);
    res.json({ success: true, message: "Token刷新成功", data: result });
  } catch (error) {
    const message = getUpstreamErrorMessage(error);
    logger.error("刷新Token失败:", message);
    const status = getUpstreamErrorStatus(error);
    res.status(status).json({ success: false, message });
  }
});

router.post(
  "/tokens/:tokenId/fetch-project-id",
  cookieAuthMiddleware,
  async (req, res) => {
    const { tokenId } = req.params;
    try {
      const result = await tokenManager.fetchProjectIdForToken(tokenId);
      logger.info(`手动获取ProjectId: ${tokenId} -> ${result.projectId}`);
      res.json({
        success: true,
        message: "Project ID获取成功",
        projectId: result.projectId,
        sub: result.sub,
        credits: result.credits,
      });
    } catch (error) {
      const message = getUpstreamErrorMessage(error);
      logger.error("获取ProjectId失败:", message);
      const status = getUpstreamErrorStatus(error);
      res.status(status).json({ success: false, message });
    }
  },
);

router.post("/tokens/export", cookieAuthMiddleware, async (req, res) => {
  const { password } = req.body;

  if (!password || !verifyPassword(password)) {
    return res.status(403).json({ success: false, message: "密码验证失败" });
  }

  try {
    const allTokens = await tokenManager.store.readAll();

    logger.info("导出所有Token数据");
    const exportData = buildAntigravityExportData(allTokens);

    res.json({ success: true, data: exportData });
  } catch (error) {
    logger.error("导出Token失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post("/tokens/import", cookieAuthMiddleware, async (req, res) => {
  const { password, data, mode = "merge" } = req.body;

  if (!password || !verifyPassword(password)) {
    return res.status(403).json({ success: false, message: "密码验证失败" });
  }

  if (!data || !data.tokens || !Array.isArray(data.tokens)) {
    return res.status(400).json({ success: false, message: "无效的导入数据格式" });
  }

  try {
    const importTokens = data.tokens;
    let addedCount = 0;
    let skippedCount = 0;
    let updatedCount = 0;

    const parsedTokens = [];
    for (const rawToken of importTokens) {
      const parsed = smartParseToken(rawToken);
      if (parsed) {
        parsedTokens.push(parsed);
      } else {
        skippedCount++;
      }
    }

    if (mode === "replace") {
      await tokenManager.store.writeAll(parsedTokens);
      addedCount = parsedTokens.length;
    } else {
      const existingTokens = await tokenManager.store.readAll();
      const existingRefreshTokens = new Set(
        existingTokens.map((token) => token.refresh_token),
      );

      for (const token of parsedTokens) {
        if (existingRefreshTokens.has(token.refresh_token)) {
          const index = existingTokens.findIndex(
            (item) => item.refresh_token === token.refresh_token,
          );
          if (index !== -1) {
            existingTokens[index] = { ...existingTokens[index], ...token };
            updatedCount++;
          }
        } else {
          existingTokens.push(token);
          addedCount++;
        }
      }

      await tokenManager.store.writeAll(existingTokens);
    }

    await tokenManager.reload();

    logger.info(
      `导入Token: 新增 ${addedCount}, 更新 ${updatedCount}, 跳过 ${skippedCount}`,
    );
    res.json({
      success: true,
      message: `导入完成：新增 ${addedCount} 个，更新 ${updatedCount} 个，跳过 ${skippedCount} 个`,
      data: { added: addedCount, updated: updatedCount, skipped: skippedCount },
    });
  } catch (error) {
    logger.error("导入Token失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post("/oauth/exchange", cookieOrPasswordAuthMiddleware, async (req, res) => {
  const {
    callbackUrl,
    code: rawCode,
    port: rawPort,
    mode = "antigravity",
  } = req.body;

  let code = rawCode;
  let port = rawPort;

  if (callbackUrl && (!code || !port)) {
    try {
      const parsedUrl = new URL(String(callbackUrl));
      code = code || parsedUrl.searchParams.get("code");
      port =
        port ||
        new URL(parsedUrl.origin).port ||
        (parsedUrl.protocol === "https:" ? 443 : 80);
    } catch {
      return res.status(400).json({ success: false, message: "callbackUrl格式无效" });
    }
  }

  if (!code || !port) {
    return res.status(400).json({
      success: false,
      message: "code和port必填，或提供完整callbackUrl",
    });
  }

  try {
    const account = await oauthManager.authenticate(code, port, mode);

    if (mode === "geminicli") {
      const saveResult = await geminicliTokenManager.addToken(account);
      if (!saveResult.success) {
        throw new Error(saveResult.message || "Gemini CLI Token保存失败");
      }

      res.json({
        success: true,
        data: account,
        message: saveResult.message || "Gemini CLI Token添加成功",
        saved: saveResult.saved !== false,
        validated: saveResult.validated !== false,
        disabled: saveResult.disabled === true,
        tokenId: saveResult.tokenId || null,
      });
    } else {
      const saveResult = await tokenManager.addToken(account);
      if (!saveResult.success) {
        throw new Error(saveResult.message || "Token保存失败");
      }

      const defaultSuccessMessage = account.hasQuota
        ? "Token添加成功"
        : "Token添加成功（该账号无资格，已自动使用随机ProjectId）";
      res.json({
        success: true,
        data: account,
        message: saveResult.message || defaultSuccessMessage,
        fallbackMode: !account.hasQuota,
        saved: saveResult.saved !== false,
        validated: saveResult.validated !== false,
        disabled: saveResult.disabled === true,
        tokenId: saveResult.tokenId || null,
      });
    }
  } catch (error) {
    logger.error(`[${mode}] 认证失败:`, error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.get("/oauth/url", cookieOrPasswordAuthMiddleware, async (req, res) => {
  const mode = req.query.mode === "geminicli" ? "geminicli" : "antigravity";
  const rawCount = Number.parseInt(String(req.query.count || "1"), 10);
  const count = Math.max(1, Math.min(100, Number.isInteger(rawCount) ? rawCount : 1));

  try {
    const ports = new Set();
    while (ports.size < count) {
      ports.add(Math.floor(Math.random() * 10000) + 50000);
    }

    const urls = Array.from(ports).map((port) => ({
      port,
      url: oauthManager.generateAuthUrl(port, mode),
    }));

    res.json({
      success: true,
      data: {
        mode,
        count,
        urls,
        submit: {
          method: "POST",
          url: "/admin/oauth/exchange",
          contentType: "application/json",
          body: {
            code: "从回调URL中提取的code",
            port: "回调URL中的本地端口",
            mode,
            password: "可选，未携带Cookie时可直接传管理员密码",
          },
        },
      },
    });
  } catch (error) {
    logger.error(`[${mode}] 获取OAuth URL失败:`, error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.get("/config", cookieAuthMiddleware, (req, res) => {
  try {
    const envData = parseEnvFile(envPath);
    const jsonData = getConfigJson();

    res.json({ success: true, data: { env: envData, json: jsonData } });
  } catch (error) {
    logger.error("读取配置失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.put("/config", cookieAuthMiddleware, (req, res) => {
  try {
    const { env: envUpdates, json: jsonUpdates, password } = req.body;

    if (jsonUpdates?.other) {
      const rawProxyPool = normalizeProxyPoolInput(jsonUpdates.other.proxyPool || "");
      const rawDisabledProxyPool = normalizeProxyPoolInput(
        jsonUpdates.other.disabledProxyPool || "",
      );
      const proxyProtocol = normalizeProxyProtocol(
        jsonUpdates.other.proxyProtocol || "http",
      );

      jsonUpdates.other.proxyProtocol = proxyProtocol;
      jsonUpdates.other.proxyPool = rawProxyPool;
      jsonUpdates.other.disabledProxyPool = rawDisabledProxyPool;

      if (rawProxyPool) {
        parseProxyPool(rawProxyPool, proxyProtocol);
      }
      if (rawDisabledProxyPool) {
        parseProxyPool(rawDisabledProxyPool, proxyProtocol);
      }
    }

    if (envUpdates && envUpdates.OFFICIAL_SYSTEM_PROMPT !== undefined) {
      const currentEnv = parseEnvFile(envPath);
      const normalizeNewlines = (str) =>
        (str || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
      const newValue = normalizeNewlines(envUpdates.OFFICIAL_SYSTEM_PROMPT);
      const oldValue = normalizeNewlines(currentEnv.OFFICIAL_SYSTEM_PROMPT);

      if (newValue !== oldValue) {
        if (!password || !verifyPassword(password)) {
          logger.warn(`尝试修改官方系统提示词但密码验证失败 IP: ${getClientIP(req)}`);
          return res.status(403).json({
            success: false,
            message: "修改官方系统提示词需要验证管理员密码",
          });
        }
      }
    }

    if (envUpdates) updateEnvFile(envPath, envUpdates);
    if (jsonUpdates) saveConfigJson(deepMerge(getConfigJson(), jsonUpdates));

    dotenv.config({ override: true });
    reloadConfig();

    memoryManager.setCleanupInterval(config.server.memoryCleanupInterval);

    logger.info("系统配置已更新并热重载");
    res.json({
      success: true,
      message: "配置已保存并生效（端口/HOST修改需重启）",
    });
  } catch (error) {
    logger.error("更新配置失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.get("/proxy-pool/disabled", cookieAuthMiddleware, (req, res) => {
  try {
    const jsonData = getConfigJson();
    const protocol = normalizeProxyProtocol(jsonData.other?.proxyProtocol || "http");
    const disabledPoolRaw = normalizeProxyPoolInput(
      jsonData.other?.disabledProxyPool || "",
    );
    const { activeEntries, disabledEntries } = getProxyPoolSummary(config.proxy);

    res.json({
      success: true,
      data: {
        proxyProtocol: protocol,
        poolRaw: disabledPoolRaw,
        count: disabledEntries.length,
        activeCount: activeEntries.length,
        entries: parseProxyPool(disabledPoolRaw, protocol).map((entry, index) => ({
          index,
          raw: entry.raw,
          url: entry.url,
          protocol: entry.protocol,
          host: entry.host,
          port: entry.port,
          username: entry.username,
        })),
      },
    });
  } catch (error) {
    logger.error("获取禁用代理池失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.put("/proxy-pool/disabled", cookieAuthMiddleware, (req, res) => {
  try {
    const currentJson = getConfigJson();
    const proxyProtocol = normalizeProxyProtocol(
      req.body?.proxyProtocol || currentJson.other?.proxyProtocol || "http",
    );
    const poolRaw = normalizeProxyPoolInput(req.body?.poolRaw || "");

    if (poolRaw) {
      parseProxyPool(poolRaw, proxyProtocol);
    }

    saveConfigJson({
      other: {
        proxyProtocol,
        disabledProxyPool: poolRaw,
      },
    });

    dotenv.config({ override: true });
    reloadConfig();

    res.json({
      success: true,
      message: "禁用代理池已更新",
      data: {
        proxyProtocol,
        poolRaw,
        count: poolRaw ? parseProxyPool(poolRaw, proxyProtocol).length : 0,
      },
    });
  } catch (error) {
    logger.error("更新禁用代理池失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post("/proxy-pool/test", cookieAuthMiddleware, async (req, res) => {
  try {
    const currentJson = getConfigJson();
    const proxyProtocol = normalizeProxyProtocol(
      req.body?.proxyProtocol || currentJson.other?.proxyProtocol || "http",
    );
    const poolRaw = normalizeProxyPoolInput(
      req.body?.poolRaw ?? currentJson.other?.proxyPool ?? "",
    );
    let disabledPoolRaw = normalizeProxyPoolInput(
      req.body?.disabledPoolRaw ?? currentJson.other?.disabledProxyPool ?? "",
    );
    const concurrencyLimit = normalizeProxyTestConcurrency(req.body?.concurrency);

    const entries = parseProxyPool(poolRaw, proxyProtocol);
    if (entries.length === 0) {
      return res.status(400).json({
        success: false,
        message: "启用代理池为空，无法执行测试",
      });
    }

    let workingPoolRaw = poolRaw;

    const results = await runWithConcurrency(entries, concurrencyLimit, async (proxyEntry) =>
      testSingleProxyConnectivity({ proxyEntry, proxyProtocol }),
    );

    for (const testResult of results) {
      if (!testResult?.shouldDisable) continue;
      const disableResult = moveProxyToDisabledPool({
        proxyEntry: {
          raw: testResult.raw,
          url: testResult.url,
          protocol: testResult.protocol,
          host: testResult.host,
          port: testResult.port,
          username: testResult.username,
          password: testResult.password,
        },
        proxyProtocol,
        poolRaw: workingPoolRaw,
        disabledPoolRaw,
        persist: false,
        reason: `代理测试返回 407: ${getProxyTestTargetUrl()}`,
      });
      testResult.disableResult = disableResult;
      testResult.autoDisabled = disableResult.changed === true;
      if (disableResult.changed) {
        workingPoolRaw = disableResult.nextPoolRaw;
        disabledPoolRaw = disableResult.nextDisabledPoolRaw;
      }
    }

    if (
      disabledPoolRaw !== normalizeProxyPoolInput(currentJson.other?.disabledProxyPool || "") ||
      workingPoolRaw !== normalizeProxyPoolInput(currentJson.other?.proxyPool || "")
    ) {
      saveConfigJson({
        other: {
          proxyProtocol,
          proxyPool: workingPoolRaw,
          disabledProxyPool: disabledPoolRaw,
        },
      });
      dotenv.config({ override: true });
      reloadConfig();
    }

    const summary = {
      tested: results.length,
      success: results.filter((item) => item.success).length,
      failed: results.filter((item) => !item.success).length,
      autoDisabled: results.filter((item) => item.autoDisabled).length,
      concurrency: concurrencyLimit,
      requestMode: getProxyTestModeLabel(),
      targetUrl: getProxyTestTargetUrl(),
      proxyProtocol,
      remainingActiveCount: countProxyPoolEntries(workingPoolRaw),
      disabledCount: countProxyPoolEntries(disabledPoolRaw),
    };

    logger.info(
      `[ProxyPool] 代理测试完成: 共 ${summary.tested} 条，成功 ${summary.success} 条，失败 ${summary.failed} 条，自动禁用 ${summary.autoDisabled} 条`,
    );

    res.json({
      success: true,
      message: "代理池测试完成",
      data: {
        summary,
        results,
        poolRaw: workingPoolRaw,
        disabledPoolRaw,
        logText: buildProxyTestLogText({ summary, results }),
        downloadFilename: `proxy-test-${Date.now()}.log`,
      },
    });
  } catch (error) {
    logger.error("代理池测试失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.get("/rotation", cookieAuthMiddleware, async (req, res) => {
  try {
    await Promise.all([
      tokenManager._ensureInitialized(),
      geminicliTokenManager._ensureInitialized(),
    ]);

    const antigravityConfig = tokenManager.getRotationConfig();
    const geminicliConfig = geminicliTokenManager.getRotationConfig();
    res.json({
      success: true,
      data: {
        ...antigravityConfig,
        antigravity: {
          ...antigravityConfig,
          progressGroups: tokenManager.getRotationProgress(
            ANTIGRAVITY_ROTATION_GROUPS,
          ),
        },
        geminicli: {
          ...geminicliConfig,
          progressGroups: geminicliTokenManager.getRotationProgress(
            GEMINICLI_ROTATION_GROUPS,
          ),
        },
      },
    });
  } catch (error) {
    logger.error("获取轮询配置失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.put("/rotation", cookieAuthMiddleware, async (req, res) => {
  try {
    const { strategy, requestCount, warmup = true } = req.body;
    const shouldWarmup = warmup !== false;

    const validStrategies = ["round_robin", "quota_exhausted", "request_count"];
    if (strategy && !validStrategies.includes(strategy)) {
      return res.status(400).json({
        success: false,
        message: `无效的策略，可选值: ${validStrategies.join(", ")}`,
      });
    }

    tokenManager.updateRotationConfig(strategy, requestCount);
    geminicliTokenManager.updateRotationConfig(strategy, requestCount);

    const currentConfig = getConfigJson();
    if (!currentConfig.rotation) currentConfig.rotation = {};
    if (strategy) currentConfig.rotation.strategy = strategy;
    if (requestCount) currentConfig.rotation.requestCount = requestCount;
    saveConfigJson(currentConfig);

    reloadConfig();

    await Promise.all([tokenManager.reload(), geminicliTokenManager.reload()]);

    if (shouldWarmup) {
      const [antigravityReadyIndices, geminicliReadyIndices] = await Promise.all([
        warmupAntigravityRotationQuotas(),
        warmupGeminiCliRotationQuotas(),
      ]);

      tokenManager.randomizeRotationStart(antigravityReadyIndices);
      geminicliTokenManager.randomizeRotationStart(geminicliReadyIndices);
    }

    logger.info(
      `轮询策略已更新: ${strategy || "未变"}, 请求次数: ${requestCount || "未变"}`,
    );
    const antigravityConfig = tokenManager.getRotationConfig();
    const geminicliConfig = geminicliTokenManager.getRotationConfig();
    res.json({
      success: true,
      message: "轮询策略已更新",
      data: {
        ...antigravityConfig,
        antigravity: {
          ...antigravityConfig,
          progressGroups: tokenManager.getRotationProgress(
            ANTIGRAVITY_ROTATION_GROUPS,
          ),
        },
        geminicli: {
          ...geminicliConfig,
          progressGroups: geminicliTokenManager.getRotationProgress(
            GEMINICLI_ROTATION_GROUPS,
          ),
        },
      },
    });
  } catch (error) {
    logger.error("更新轮询配置失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.get("/logs", cookieAuthMiddleware, (req, res) => {
  try {
    const { level, search, limit, offset } = req.query;
    const options = {
      level: level || "all",
      search: search || "",
      limit: parseInt(limit, 10) || 100,
      offset: parseInt(offset, 10) || 0,
    };

    const result = logger.getLogs(options);
    res.json({ success: true, data: result });
  } catch (error) {
    logger.error("获取日志失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.get("/logs/stats", cookieAuthMiddleware, (req, res) => {
  try {
    const stats = logger.getLogStats();
    res.json({ success: true, data: stats });
  } catch (error) {
    logger.error("获取日志统计失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.delete("/logs", cookieAuthMiddleware, (req, res) => {
  try {
    logger.clearLogs();
    logger.info("日志已清空");
    res.json({ success: true, message: "日志已清空" });
  } catch (error) {
    logger.error("清空日志失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.get("/geminicli/tokens", cookieAuthMiddleware, async (req, res) => {
  try {
    const tokens = await geminicliTokenManager.getTokenList();
    res.json({ success: true, data: tokens });
  } catch (error) {
    logger.error("[GeminiCLI] 获取Token列表失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post("/geminicli/tokens", cookieAuthMiddleware, async (req, res) => {
  const { access_token, refresh_token, expires_in, timestamp, enable, email } = req.body;
  if (!access_token || !refresh_token) {
    return res
      .status(400)
      .json({ success: false, message: "access_token和refresh_token必填" });
  }
  const tokenData = { access_token, refresh_token, expires_in };
  if (timestamp) tokenData.timestamp = timestamp;
  if (enable !== undefined) tokenData.enable = enable;
  if (email) tokenData.email = email;

  try {
    const result = await geminicliTokenManager.addToken(tokenData);
    logger.info(`[GeminiCLI] 添加新Token: ${access_token.substring(0, 8)}...`);
    res.json(result);
  } catch (error) {
    logger.error("[GeminiCLI] 添加Token失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.put("/geminicli/tokens/:tokenId", cookieAuthMiddleware, async (req, res) => {
  const { tokenId } = req.params;
  const updates = req.body;

  try {
    if (updates.enable === true && updates.enableWithTest) {
      delete updates.enableWithTest;
      const result = await geminicliTokenManager.enableTokenById(tokenId);
      if (result.success) {
        logger.info(`[GeminiCLI] 启用Token(已验证): ${tokenId}`);
      } else {
        logger.warn(`[GeminiCLI] 启用Token验证失败: ${tokenId} - ${result.message}`);
      }
      res.json(result);
    } else {
      const result = await geminicliTokenManager.updateTokenById(tokenId, updates);
      logger.info(`[GeminiCLI] 更新Token: ${tokenId}`);
      res.json(result);
    }
  } catch (error) {
    const message = getUpstreamErrorMessage(error);
    logger.error("[GeminiCLI] 更新Token失败:", message);
    res.status(getUpstreamErrorStatus(error)).json({ success: false, message });
  }
});

router.delete("/geminicli/tokens/:tokenId", cookieAuthMiddleware, async (req, res) => {
  const { tokenId } = req.params;
  try {
    const result = await geminicliTokenManager.deleteTokenById(tokenId);
    logger.info(`[GeminiCLI] 删除Token: ${tokenId}`);
    res.json(result);
  } catch (error) {
    logger.error("[GeminiCLI] 删除Token失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post("/geminicli/tokens/reload", cookieAuthMiddleware, async (req, res) => {
  try {
    await geminicliTokenManager.reload();
    logger.info("[GeminiCLI] 手动触发Token热重载");
    res.json({ success: true, message: "Gemini CLI Token已热重载" });
  } catch (error) {
    logger.error("[GeminiCLI] 热重载失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post("/geminicli/tokens/batch", cookieAuthMiddleware, async (req, res) => {
  const action = String(req.body?.action || "").trim();
  const tokenIds = normalizeBatchTokenIds(req.body?.tokenIds);

  if (!action) {
    return res.status(400).json({ success: false, message: "action必填" });
  }

  if (tokenIds.length === 0) {
    return res.status(400).json({ success: false, message: "tokenIds不能为空" });
  }

  try {
    if (action === "export") {
      const { password } = req.body || {};
      if (!password || !verifyPassword(password)) {
        return res.status(403).json({ success: false, message: "密码验证失败" });
      }

      const { results, tokens } = await buildBatchExportResults(tokenIds, (tokenId) =>
        geminicliTokenManager.findTokenById(tokenId),
      );
      const payload = buildBatchResponsePayload(action, results, {
        exportData: buildGeminiCliExportData(tokens),
      });

      return res.json({
        success: true,
        message: `批量导出完成：成功 ${payload.successCount} 个，失败 ${payload.failCount} 个`,
        data: payload,
      });
    }

    const results = [];

    for (const tokenId of tokenIds) {
      try {
        let result;

        if (action === "enable") {
          result = await geminicliTokenManager.enableTokenById(tokenId, {
            stage: "manual",
          });
        } else if (action === "disable") {
          const now = Date.now();
          result = await geminicliTokenManager.updateTokenById(tokenId, {
            enable: false,
            disableReason: "手动批量禁用",
            disableTime: now,
            lastError: "手动批量禁用",
            lastErrorTime: now,
            lastErrorStage: "manual",
          });
        } else if (action === "fetch_project_id") {
          const projectResult = await geminicliTokenManager.fetchProjectIdForToken(tokenId);
          result = {
            success: true,
            message: "Project ID获取成功",
            projectId: projectResult.projectId,
            tier: projectResult.tier || null,
          };
        } else if (action === "refresh_token") {
          const refreshResult = await geminicliTokenManager.refreshTokenById(tokenId);
          result = {
            success: true,
            message: "Token刷新成功",
            data: refreshResult,
          };
        } else if (action === "refresh_quota") {
          const quotas = await refreshGeminiCliQuotaByTokenId(tokenId);
          result = {
            success: true,
            message: "额度刷新成功",
            modelCount: Object.keys(quotas || {}).length,
          };
        } else if (action === "delete") {
          result = await geminicliTokenManager.deleteTokenById(tokenId);
        } else {
          return res.status(400).json({
            success: false,
            message: `不支持的批量操作: ${action}`,
          });
        }

        results.push({
          tokenId,
          success: result.success !== false,
          message: result.message || "操作成功",
          ...(result.projectId ? { projectId: result.projectId } : {}),
          ...(result.tier ? { tier: result.tier } : {}),
          ...(result.data ? { data: result.data } : {}),
          ...(result.modelCount !== undefined ? { modelCount: result.modelCount } : {}),
        });
      } catch (error) {
        results.push({
          tokenId,
          success: false,
          message: getUpstreamErrorMessage(error, "操作失败"),
        });
      }
    }

    const payload = buildBatchResponsePayload(action, results);
    res.json({
      success: true,
      message: `批量操作完成：成功 ${payload.successCount} 个，失败 ${payload.failCount} 个`,
      data: payload,
    });
  } catch (error) {
    logger.error("批量操作Gemini CLI Token失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post(
  "/geminicli/tokens/:tokenId/refresh",
  cookieAuthMiddleware,
  async (req, res) => {
    const { tokenId } = req.params;
    try {
      const result = await geminicliTokenManager.refreshTokenById(tokenId);
      logger.info(`[GeminiCLI] 手动刷新Token: ${tokenId}`);
      res.json({ success: true, message: "Token刷新成功", data: result });
    } catch (error) {
      const message = getUpstreamErrorMessage(error);
      logger.error("[GeminiCLI] 刷新Token失败:", message);
      const status = getUpstreamErrorStatus(error);
      res.status(status).json({ success: false, message });
    }
  },
);

router.post(
  "/geminicli/tokens/:tokenId/fetch-project-id",
  cookieAuthMiddleware,
  async (req, res) => {
    const { tokenId } = req.params;
    try {
      const result = await geminicliTokenManager.fetchProjectIdForToken(tokenId);
      logger.info(
        `[GeminiCLI] 手动获取ProjectId: ${tokenId} -> ${result.projectId} (tier: ${result.tier || "unknown"})`,
      );
      res.json({
        success: true,
        message: `Project ID获取成功${result.tier ? ` (tier: ${result.tier})` : ""}`,
        projectId: result.projectId,
        tier: result.tier || null,
      });
    } catch (error) {
      const message = getUpstreamErrorMessage(error);
      logger.error("[GeminiCLI] 获取ProjectId失败:", message);
      const status = getUpstreamErrorStatus(error);
      res.status(status).json({ success: false, message });
    }
  },
);

router.post(
  "/geminicli/tokens/batch-fetch-project-ids",
  cookieAuthMiddleware,
  async (req, res) => {
    try {
      const result = await geminicliTokenManager.batchFetchProjectIds();
      logger.info(
        `[GeminiCLI] 批量获取ProjectId完成: 成功 ${result.successCount} 个，失败 ${result.failCount} 个，共 ${result.total} 个`,
      );
      res.json({
        success: true,
        message: `批量获取完成: 成功 ${result.successCount} 个，失败 ${result.failCount} 个`,
        ...result,
      });
    } catch (error) {
      const message = getUpstreamErrorMessage(error);
      logger.error("[GeminiCLI] 批量获取ProjectId失败:", message);
      const status = getUpstreamErrorStatus(error);
      res.status(status).json({ success: false, message });
    }
  },
);

router.get("/geminicli/tokens/:tokenId/quotas", cookieAuthMiddleware, async (req, res) => {
  let tokenData;
  try {
    const { tokenId } = req.params;
    const forceRefresh = req.query.refresh === "true";

    tokenData = await geminicliTokenManager.findTokenById(tokenId);
    if (!tokenData) {
      return res.status(404).json({ success: false, message: "Token不存在" });
    }

    const isDisabled = tokenData.enable === false;
    let quotaData = quotaManager.getQuota(tokenId);

    if (isDisabled) {
      if (!quotaData) {
        quotaData = { lastUpdated: null, models: {}, requestCounts: {} };
      }
    } else {
      if (geminicliTokenManager.isExpired(tokenData)) {
        try {
          tokenData = await geminicliTokenManager.refreshToken(tokenData);
        } catch (error) {
          logger.error("[GeminiCLI] 刷新token失败:", error.message);
          return res.status(400).json({
            success: false,
            message: "Google Token已过期且刷新失败，请重新登录Google账号",
          });
        }
      }

      if (forceRefresh) {
        quotaData = null;
      }

      if (!quotaData) {
        const quotas = await getGeminiCliQuotas(tokenData);
        quotaManager.updateQuota(tokenId, quotas, "geminicli");
        quotaData = quotaManager.getQuota(tokenId) || {
          lastUpdated: Date.now(),
          models: quotas,
          requestCounts: {},
        };
      }
    }

    const modelsWithBeijingTime = {};
    Object.entries(quotaData.models || {}).forEach(([modelId, quota]) => {
      modelsWithBeijingTime[modelId] = {
        remaining: quota.r,
        resetTime: quotaManager.convertToBeijingTime(quota.t),
        resetTimeRaw: quota.t,
      };
    });

    res.json({
      success: true,
      data: {
        lastUpdated: quotaData.lastUpdated,
        models: modelsWithBeijingTime,
        requestCounts: quotaData.requestCounts || {},
      },
    });
  } catch (error) {
    const status =
      error.statusCode || error.status || error.response?.status || 500;
    const responseData =
      typeof error.response?.data === "string"
        ? error.response.data
        : error.response?.data
          ? JSON.stringify(error.response.data)
          : "";
    const responseHeaders = error.response?.headers
      ? JSON.stringify(error.response.headers)
      : "";
    logger.error(
      `[GeminiCLI] 获取额度失败 | status=${status} | tokenId=${req.params.tokenId} | email=${tokenData?.email || ""} | projectId=${tokenData?.projectId || ""} | response=${responseData || "(empty)"} | headers=${responseHeaders || "(empty)"} | message=${error.message}`,
    );
    res.status(status).json({ success: false, message: error.message });
  }
});

router.post("/geminicli/tokens/export", cookieAuthMiddleware, async (req, res) => {
  const { password } = req.body;

  if (!password || !verifyPassword(password)) {
    return res.status(403).json({ success: false, message: "密码验证失败" });
  }

  try {
    const allTokens = await geminicliTokenManager.store.readAll();

    logger.info("[GeminiCLI] 导出所有Token数据");
    const exportData = buildGeminiCliExportData(allTokens);

    res.json({ success: true, data: exportData });
  } catch (error) {
    logger.error("[GeminiCLI] 导出Token失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post("/geminicli/tokens/import", cookieAuthMiddleware, async (req, res) => {
  const { password, data, mode = "merge" } = req.body;

  if (!password || !verifyPassword(password)) {
    return res.status(403).json({ success: false, message: "密码验证失败" });
  }

  const importList = extractGeminiCliImportList(data);

  if (!Array.isArray(importList)) {
    return res.status(400).json({ success: false, message: "无效的导入数据格式" });
  }

  try {
    const importTokens = importList;
    let addedCount = 0;
    let skippedCount = 0;
    let updatedCount = 0;

    const parsedTokens = [];
    for (const rawToken of importTokens) {
      const parsed = smartParseGeminiCliToken(rawToken);
      if (parsed) parsedTokens.push(parsed);
      else skippedCount++;
    }

    if (mode === "replace") {
      await geminicliTokenManager.store.writeAll(parsedTokens);
      addedCount = parsedTokens.length;
    } else {
      const existingTokens = await geminicliTokenManager.store.readAll();
      const existingRefreshTokens = new Set(
        existingTokens.map((token) => token.refresh_token),
      );

      for (const token of parsedTokens) {
        if (existingRefreshTokens.has(token.refresh_token)) {
          const index = existingTokens.findIndex(
            (item) => item.refresh_token === token.refresh_token,
          );
          if (index !== -1) {
            existingTokens[index] = { ...existingTokens[index], ...token };
            updatedCount++;
          }
        } else {
          existingTokens.push(token);
          addedCount++;
        }
      }

      await geminicliTokenManager.store.writeAll(existingTokens);
    }

    await geminicliTokenManager.reload();

    logger.info(
      `[GeminiCLI] 导入Token: 新增 ${addedCount}, 更新 ${updatedCount}, 跳过 ${skippedCount}`,
    );
    res.json({
      success: true,
      message: `导入完成：新增 ${addedCount} 个，更新 ${updatedCount} 个，跳过 ${skippedCount} 个`,
      data: {
        added: addedCount,
        updated: updatedCount,
        skipped: skippedCount,
      },
    });
  } catch (error) {
    logger.error("[GeminiCLI] 导入Token失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.get("/blocked-ips", cookieAuthMiddleware, async (req, res) => {
  try {
    const list = await ipBlockManager.listBlocked();
    res.json({ success: true, data: list });
  } catch (error) {
    logger.error("获取封禁列表失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post("/unblock-ip", cookieAuthMiddleware, async (req, res) => {
  try {
    const { ip } = req.body;
    if (!ip) {
      return res.status(400).json({ success: false, message: "IP地址必填" });
    }
    const success = await ipBlockManager.unblock(ip);
    if (success) {
      res.json({ success: true, message: `IP ${ip} 已解除封禁` });
    } else {
      res.status(404).json({ success: false, message: "IP不在封禁列表中" });
    }
  } catch (error) {
    logger.error("解除封禁失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.get("/security-config", cookieAuthMiddleware, async (req, res) => {
  try {
    const securityConfig = ipBlockManager.getConfig();
    res.json({ success: true, data: securityConfig });
  } catch (error) {
    logger.error("获取安全配置失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.put("/security-config", cookieAuthMiddleware, async (req, res) => {
  try {
    const { config: updates } = req.body;
    const currentConfig = ipBlockManager.getConfig();
    const mergedConfig = deepMerge(currentConfig, updates);
    await ipBlockManager.updateConfig(mergedConfig);
    res.json({ success: true, message: "安全配置已更新" });
  } catch (error) {
    logger.error("更新安全配置失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post("/whitelist-ip", cookieAuthMiddleware, async (req, res) => {
  try {
    const { ip } = req.body;
    if (!ip) {
      return res.status(400).json({ success: false, message: "IP地址必填" });
    }
    const success = await ipBlockManager.addWhitelistIP(ip);
    if (success) {
      res.json({ success: true, message: `IP ${ip} 已添加到白名单` });
    } else {
      res.json({ success: false, message: "IP已在白名单中" });
    }
  } catch (error) {
    logger.error("添加白名单失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.delete("/whitelist-ip", cookieAuthMiddleware, async (req, res) => {
  try {
    const { ip } = req.body;
    if (!ip) {
      return res.status(400).json({ success: false, message: "IP地址必填" });
    }
    const success = await ipBlockManager.removeWhitelistIP(ip);
    if (success) {
      res.json({ success: true, message: `IP ${ip} 已从白名单移除` });
    } else {
      res.status(404).json({ success: false, message: "IP不在白名单中" });
    }
  } catch (error) {
    logger.error("移除白名单失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.get("/tokens/:tokenId/quotas", cookieAuthMiddleware, async (req, res) => {
  try {
    const { tokenId } = req.params;
    const forceRefresh = req.query.refresh === "true";

    let tokenData = await tokenManager.findTokenById(tokenId);

    if (!tokenData) {
      return res.status(404).json({ success: false, message: "Token不存在" });
    }

    const isDisabled = tokenData.enable === false;
    let quotaData = quotaManager.getQuota(tokenId);

    if (isDisabled) {
      if (!quotaData) {
        quotaData = { lastUpdated: null, models: {} };
      }
    } else {
      if (tokenManager.isExpired(tokenData)) {
        try {
          tokenData = await tokenManager.refreshToken(tokenData);
        } catch (error) {
          logger.error("刷新token失败:", error.message);
          return res.status(400).json({
            success: false,
            message: "Google Token已过期且刷新失败，请重新登录Google账号",
          });
        }
      }

      if (forceRefresh) {
        quotaData = null;
        try {
          const subscription = await tokenManager.fetchSubscriptionAndCredits(tokenData);
          if (subscription?.sub !== undefined) {
            tokenData.sub = subscription.sub;
          }
          if (subscription?.credits !== undefined) {
            tokenData.credits = subscription.credits;
          }
          tokenManager.saveToFile(tokenData);

          const memoryToken = tokenManager.tokens.find(
            (item) => item.refresh_token === tokenData.refresh_token,
          );
          if (memoryToken) {
            memoryToken.sub = tokenData.sub;
            memoryToken.credits = tokenData.credits;
          }
        } catch (error) {
          logger.warn(`刷新额度时同步积分失败: ${error.message}`);
        }
      }

      if (!quotaData) {
        const quotas = await getModelsWithQuotas(tokenData);
        quotaManager.updateQuota(tokenId, quotas);
        quotaData = quotaManager.getQuota(tokenId) || {
          lastUpdated: Date.now(),
          models: quotas,
          requestCounts: {},
        };
      }
    }

    const modelsWithBeijingTime = {};
    Object.entries(quotaData.models).forEach(([modelId, quota]) => {
      modelsWithBeijingTime[modelId] = {
        remaining: quota.r,
        resetTime: quotaManager.convertToBeijingTime(quota.t),
        resetTimeRaw: quota.t,
      };
    });

    const requestCounts = quotaData.requestCounts || {};
    const normalizedSub = tokenData?.sub || "free-tier";
    const normalizedCredits =
      tokenData?.credits ?? (normalizedSub === "free-tier" ? 0 : null);

    res.json({
      success: true,
      data: {
        lastUpdated: quotaData.lastUpdated,
        models: modelsWithBeijingTime,
        requestCounts,
        tokenState: {
          id: tokenId,
          sub: normalizedSub,
          credits: normalizedCredits,
          projectId: tokenData?.projectId || null,
          enable: tokenData?.enable !== false,
        },
      },
    });
  } catch (error) {
    logger.error("获取额度失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

export default router;
