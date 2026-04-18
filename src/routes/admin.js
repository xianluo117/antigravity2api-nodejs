import dotenv from "dotenv";
import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import { getModelsWithQuotas } from "../api/client.js";
import { getGeminiCliQuotas } from "../api/geminicli_client.js";
import geminicliTokenManager from "../auth/geminicli_token_manager.js";
import { generateToken, verifyToken } from "../auth/jwt.js";
import oauthManager from "../auth/oauth_manager.js";
import quotaManager from "../auth/quota_manager.js";
import tokenManager from "../auth/token_manager.js";
import config, { getConfigJson, saveConfigJson } from "../config/config.js";
import fingerprintRequester from "../requester.js";
import { reloadConfig } from "../utils/configReloader.js";
import { deepMerge } from "../utils/deepMerge.js";
import { parseEnvFile, updateEnvFile } from "../utils/envParser.js";
import { httpRequest } from "../utils/httpClient.js";
import ipBlockManager from "../utils/ipBlockManager.js";
import logger from "../utils/logger.js";
import memoryManager from "../utils/memoryManager.js";
import { getEnvPath } from "../utils/paths.js";
import {
  getProxyPoolSummary,
  moveProxyToDisabledPool,
  normalizeProxyPoolInput,
  normalizeProxyProtocol,
  parseProxyPool,
} from "../utils/proxyPool.js";

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
const CODE_ASSIST_AUTH_TARGET_PREFIX =
  "https://developers.google.com/gemini-code-assist/auth/";
const URL_MATCH_REGEX = /https?:\/\/[^\s"'<>]+/gi;

function getRequestPassword(req) {
  if (typeof req.body?.password === "string") {
    return req.body.password;
  }
  if (typeof req.query?.password === "string") {
    return req.query.password;
  }
  return null;
}

function getRequestEmail(req) {
  if (typeof req.query?.email === "string") {
    return req.query.email;
  }
  if (typeof req.body?.email === "string") {
    return req.body.email;
  }

  try {
    const rawUrl = String(req.originalUrl || req.url || "");
    const queryIndex = rawUrl.indexOf("?");
    if (queryIndex >= 0) {
      const searchParams = new URLSearchParams(rawUrl.slice(queryIndex + 1));
      const email = searchParams.get("email");
      if (email) {
        return email;
      }
    }
  } catch {
    // ignore
  }

  return "";
}

function getRequestMode(req, defaultMode = "auto") {
  if (typeof req.body?.mode === "string") {
    return req.body.mode.trim().toLowerCase() || defaultMode;
  }
  if (typeof req.query?.mode === "string") {
    return req.query.mode.trim().toLowerCase() || defaultMode;
  }
  return defaultMode;
}

function normalizeForbiddenMessage(value) {
  return String(value || "").replace(/\\\//g, "/");
}

function decodeTextVariants(value, maxDepth = 2) {
  const normalized = normalizeForbiddenMessage(value);
  if (!normalized) return [];

  const variants = new Set([normalized]);
  let current = normalized;

  for (let i = 0; i < maxDepth; i += 1) {
    try {
      const decoded = decodeURIComponent(current);
      if (!decoded || variants.has(decoded)) break;
      variants.add(decoded);
      current = decoded;
    } catch {
      break;
    }
  }

  return Array.from(variants);
}

function urlTargetsCodeAssistAuth(rawUrl) {
  if (!rawUrl) return false;

  const candidates = decodeTextVariants(rawUrl);
  return candidates.some((candidate) => {
    if (candidate.includes(CODE_ASSIST_AUTH_TARGET_PREFIX)) {
      return true;
    }

    try {
      const parsed = new URL(candidate);
      const continueUrl = parsed.searchParams.get("continue");
      if (!continueUrl) return false;
      return decodeTextVariants(continueUrl).some((item) =>
        item.includes(CODE_ASSIST_AUTH_TARGET_PREFIX),
      );
    } catch {
      return false;
    }
  });
}

function extractForbiddenAuthUrl(message) {
  for (const text of decodeTextVariants(message)) {
    const urls = text.match(URL_MATCH_REGEX) || [];
    for (const rawUrl of urls) {
      const cleanedUrl = rawUrl.replace(/[),.;]+$/g, "");
      if (urlTargetsCodeAssistAuth(cleanedUrl)) {
        return cleanedUrl;
      }
    }
  }
  return null;
}

function getForbiddenGoogleAuthMatch(token) {
  const candidates = [
    { field: "disableReason", value: token?.disableReason },
    { field: "lastError", value: token?.lastError },
  ];

  for (const candidate of candidates) {
    const normalized = normalizeForbiddenMessage(candidate.value);
    if (!/\b403\b/.test(normalized)) continue;

    const authUrl = extractForbiddenAuthUrl(normalized);
    if (!authUrl) continue;

    return {
      field: candidate.field,
      message: normalized,
      authUrl,
    };
  }

  return null;
}

function buildForbiddenGoogleAccountList(
  tokens,
  source,
  preferredEmails = new Set(),
) {
  const seenEmails = new Set(
    [...preferredEmails]
      .filter(Boolean)
      .map((email) => String(email).trim().toLowerCase()),
  );
  const items = [];
  let duplicateCount = 0;

  for (const token of Array.isArray(tokens) ? tokens : []) {
    const email = typeof token?.email === "string" ? token.email.trim() : "";
    if (!email) continue;

    const match = getForbiddenGoogleAuthMatch(token);
    if (!match) continue;

    const normalizedEmail = email.toLowerCase();
    if (seenEmails.has(normalizedEmail)) {
      duplicateCount += 1;
      continue;
    }

    seenEmails.add(normalizedEmail);
    items.push({
      email,
      url: match.authUrl,
      enable: token.enable !== false,
    });
  }

  return {
    items,
    duplicateCount,
    seenEmails,
  };
}

function normalizeBatchTokenIds(tokenIds) {
  if (!Array.isArray(tokenIds)) return [];
  return Array.from(
    new Set(
      tokenIds.map((tokenId) => String(tokenId || "").trim()).filter(Boolean),
    ),
  );
}

function buildBatchResponsePayload(action, results, extra = {}) {
  const list = Array.isArray(results) ? results : [];
  const successCount = list.filter((item) => item.success).length;
  const failCount = list.length - successCount;
  return {
    action,
    total: list.length,
    successCount,
    failCount,
    results: list,
    ...extra,
  };
}

async function refreshAntigravityQuotaByTokenId(tokenId) {
  let tokenData = await tokenManager.findTokenById(tokenId);
  if (!tokenData) {
    throw new Error("Token不存在");
  }
  if (tokenData.enable === false) {
    throw new Error("Token已禁用，无法刷新额度");
  }

  if (tokenManager.isExpired(tokenData)) {
    tokenData = await tokenManager.refreshToken(tokenData);
  }

  if (!tokenData.projectId) {
    await tokenManager.fetchProjectIdForToken(tokenId);
    tokenData = await tokenManager.findTokenById(tokenId);
  }

  if (!tokenData?.projectId) {
    throw new Error("缺少Project ID，无法刷新额度");
  }

  const quotas = await getModelsWithQuotas(tokenData);
  quotaManager.updateQuota(tokenId, quotas);
  return quotas;
}

async function refreshGeminiCliQuotaByTokenId(tokenId) {
  let tokenData = await geminicliTokenManager.findTokenById(tokenId);
  if (!tokenData) {
    throw new Error("Token不存在");
  }
  if (tokenData.enable === false) {
    throw new Error("Token已禁用，无法刷新额度");
  }

  if (geminicliTokenManager.isExpired(tokenData)) {
    tokenData = await geminicliTokenManager.refreshToken(tokenData);
  }

  if (!tokenData.projectId) {
    await geminicliTokenManager.fetchProjectIdForToken(tokenId);
    tokenData = await geminicliTokenManager.findTokenById(tokenId);
  }

  const quotas = await getGeminiCliQuotas(tokenData);
  quotaManager.updateQuota(tokenId, quotas, "geminicli");
  return quotas;
}

function buildAntigravityExportData(tokens) {
  return {
    version: 1,
    exportTime: new Date().toISOString(),
    tokens: tokens.map((token) => ({
      access_token: token.access_token,
      refresh_token: token.refresh_token,
      expires_in: token.expires_in,
      timestamp: token.timestamp,
      enable: token.enable,
      projectId: token.projectId,
      email: token.email,
      hasQuota: token.hasQuota,
      sub: token.sub,
    })),
  };
}

function buildGeminiCliExportData(tokens) {
  return {
    version: 1,
    exportTime: new Date().toISOString(),
    tokens: tokens.map((token) => ({
      access_token: token.access_token,
      refresh_token: token.refresh_token,
      expires_in: token.expires_in,
      timestamp: token.timestamp,
      enable: token.enable,
      email: token.email,
      projectId: token.projectId,
      tier: token.tier,
    })),
  };
}

async function buildBatchExportResults(tokenIds, findTokenById) {
  const results = [];
  const tokens = [];

  for (const tokenId of tokenIds) {
    try {
      const token = await findTokenById(tokenId);
      if (!token) {
        results.push({ success: false, tokenId, message: "Token不存在" });
        continue;
      }
      tokens.push(token);
      results.push({ success: true, tokenId, message: "已加入导出列表" });
    } catch (error) {
      results.push({
        success: false,
        tokenId,
        message: error.message || "导出失败",
      });
    }
  }

  return { results, tokens };
}

function findTokenByEmail(tokens, email) {
  const normalizedEmail = String(email || "")
    .trim()
    .toLowerCase();
  if (!normalizedEmail) return null;

  return (
    (Array.isArray(tokens) ? tokens : []).find(
      (token) =>
        String(token?.email || "")
          .trim()
          .toLowerCase() === normalizedEmail,
    ) || null
  );
}

function getRotationTokenLabel(tokenId) {
  return tokenId ? tokenId.slice(0, 12) : "unknown-token";
}

async function warmupAntigravityRotationQuotas() {
  const enabledTokens = [...tokenManager.tokens];
  const successfulIndices = [];

  for (const token of enabledTokens) {
    let activeToken = token;
    const tokenId =
      activeToken.tokenId || (await tokenManager.getTokenId(activeToken));

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
      activeToken.tokenId ||
      (await geminicliTokenManager.getTokenId(activeToken));

    if (!tokenId) continue;

    try {
      if (geminicliTokenManager.isExpired(activeToken)) {
        activeToken = await geminicliTokenManager.refreshToken(
          activeToken,
          true,
        );
      }

      if (!activeToken.projectId) {
        await geminicliTokenManager.fetchProjectIdForTokenData(
          activeToken,
          tokenId,
        );
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
    : path.join(__dirname, "..", "bin", "tls_config.json");

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
      result.logLines.push(
        "[request] transport=Axios method=POST no-credential",
      );
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

// 禁用缓存中间件，确保管理后台数据实时性
router.use((req, res, next) => {
  res.set(
    "Cache-Control",
    "no-store, no-cache, must-revalidate, proxy-revalidate",
  );
  res.set("Pragma", "no-cache");
  res.set("Expires", "0");
  next();
});

// Cookie 配置
const COOKIE_OPTIONS = {
  httpOnly: true,
  // secure: process.env.NODE_ENV === 'production', // 移除静态配置，改为动态判断
  sameSite: "strict",
  maxAge: 24 * 60 * 60 * 1000, // 24小时
};

// 从 Cookie 或 Header 获取 JWT Token 的中间件
const cookieAuthMiddleware = (req, res, next) => {
  // 优先从 Cookie 获取
  let token = req.cookies?.authToken;

  // 如果 Cookie 中没有，尝试从 Header 获取（兼容旧版本）
  if (!token) {
    const authHeader = req.headers.authorization;
    token = authHeader?.startsWith("Bearer ") ? authHeader.slice(7) : null;
  }

  if (!token) {
    return res.status(401).json({ error: "Token required" });
  }

  try {
    const decoded = verifyToken(token);
    req.user = decoded;
    next();
  } catch (error) {
    // 清除无效的 Cookie
    res.clearCookie("authToken", {
      ...COOKIE_OPTIONS,
      secure: req.secure || process.env.NODE_ENV === "production",
    });
    return res.status(401).json({ error: "Invalid token" });
  }
};

const cookieOrPasswordAuthMiddleware = (req, res, next) => {
  let token = req.cookies?.authToken;

  if (!token) {
    const authHeader = req.headers.authorization;
    token = authHeader?.startsWith("Bearer ") ? authHeader.slice(7) : null;
  }

  if (token) {
    try {
      const decoded = verifyToken(token);
      req.user = decoded;
      return next();
    } catch (error) {
      res.clearCookie("authToken", {
        ...COOKIE_OPTIONS,
        secure: req.secure || process.env.NODE_ENV === "production",
      });
    }
  }

  const password =
    typeof req.body?.password === "string"
      ? req.body.password
      : typeof req.query?.password === "string"
        ? req.query.password
        : null;

  if (password && verifyPassword(password)) {
    req.user = { username: config.admin.username, role: "admin_password" };
    return next();
  }

  return res.status(401).json({ error: "Token required or password invalid" });
};

// 获取客户端 IP
function getClientIP(req) {
  return (
    req.headers["x-forwarded-for"]?.split(",")[0]?.trim() ||
    req.headers["x-real-ip"] ||
    req.connection?.remoteAddress ||
    req.ip ||
    "unknown"
  );
}

// 登录接口
router.post("/login", async (req, res) => {
  const clientIP = getClientIP(req);

  // 使用统一的 IP 封禁管理器检查
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

  // 验证输入
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

  // 限制输入长度防止 DoS
  if (username.length > 100 || password.length > 100) {
    return res.status(400).json({ success: false, message: "输入过长" });
  }

  if (
    username === config.admin.username &&
    password === config.admin.password
  ) {
    const token = generateToken({ username, role: "admin" });

    // 设置 HttpOnly Cookie
    // 动态设置 secure: 如果通过 https 访问 (req.secure) 或在生产环境，则启用 secure
    res.cookie("authToken", token, {
      ...COOKIE_OPTIONS,
      secure: req.secure || process.env.NODE_ENV === "production",
    });

    // 同时返回 token（兼容旧版本前端）
    logger.info(`管理员登录成功 IP: ${clientIP}`);
    res.json({ success: true, token });
  } else {
    // 使用统一的 IP 封禁管理器记录违规
    await ipBlockManager.recordViolation(clientIP, "admin_login_fail");
    logger.warn(`管理员登录失败 IP: ${clientIP}`);
    res.status(401).json({ success: false, message: "用户名或密码错误" });
  }
});

// 登出接口
router.post("/logout", (req, res) => {
  res.clearCookie("authToken", {
    ...COOKIE_OPTIONS,
    secure: req.secure || process.env.NODE_ENV === "production",
  });
  res.json({ success: true, message: "已登出" });
});

// 验证密码（用于敏感操作）
function verifyPassword(password) {
  return password === config.admin.password;
}

// Token管理API - 需要JWT认证（使用 Cookie 优先）
router.get("/tokens", cookieAuthMiddleware, async (req, res) => {
  try {
    const tokens = await tokenManager.getTokenList();
    res.json({ success: true, data: tokens });
  } catch (error) {
    logger.error("获取Token列表失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.get(
  "/token-summary",
  cookieOrPasswordAuthMiddleware,
  async (req, res) => {
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
  },
);

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
        const antigravityResult = await runEnable(
          "antigravity",
          antigravityToken,
        );

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
  } = req.body;
  if (!access_token || !refresh_token) {
    return res
      .status(400)
      .json({ success: false, message: "access_token和refresh_token必填" });
  }
  const tokenData = { access_token, refresh_token, expires_in, sub };
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

// 使用 tokenId 替代 refreshToken
router.put("/tokens/:tokenId", cookieAuthMiddleware, async (req, res) => {
  const { tokenId } = req.params;
  const updates = req.body;

  // 不允许通过 API 更新敏感字段
  delete updates.access_token;
  delete updates.refresh_token;

  try {
    // 如果是从禁用状态启用，且请求中包含 enableWithTest 标志，则先测试可用性
    if (updates.enable === true && updates.enableWithTest) {
      delete updates.enableWithTest; // 不传递给底层
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
    logger.error("更新Token失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
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
    return res
      .status(400)
      .json({ success: false, message: "tokenIds不能为空" });
  }

  try {
    if (action === "export") {
      const { password } = req.body || {};
      if (!password || !verifyPassword(password)) {
        return res
          .status(403)
          .json({ success: false, message: "密码验证失败" });
      }

      const { results, tokens } = await buildBatchExportResults(
        tokenIds,
        (tokenId) => tokenManager.findTokenById(tokenId),
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
          result = await tokenManager.enableTokenById(tokenId, {
            stage: "manual",
          });
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
          const projectResult =
            await tokenManager.fetchProjectIdForToken(tokenId);
          result = {
            success: true,
            message: "Project ID获取成功",
            projectId: projectResult.projectId,
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
          ...(result.data ? { data: result.data } : {}),
          ...(result.modelCount !== undefined
            ? { modelCount: result.modelCount }
            : {}),
        });
      } catch (error) {
        results.push({
          tokenId,
          success: false,
          message: error.message || "操作失败",
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

// 刷新指定Token的access_token（使用 tokenId）
router.post(
  "/tokens/:tokenId/refresh",
  cookieAuthMiddleware,
  async (req, res) => {
    const { tokenId } = req.params;
    try {
      const result = await tokenManager.refreshTokenById(tokenId);
      logger.info(`手动刷新Token: ${tokenId}`);
      res.json({ success: true, message: "Token刷新成功", data: result });
    } catch (error) {
      logger.error("刷新Token失败:", error.message);
      const status = error.statusCode || 500;
      res.status(status).json({ success: false, message: error.message });
    }
  },
);

// 手动获取指定Token的Project ID（使用 tokenId）
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
      });
    } catch (error) {
      logger.error("获取ProjectId失败:", error.message);
      const status = error.statusCode || 500;
      res.status(status).json({ success: false, message: error.message });
    }
  },
);

// 导出所有 Token（需要密码验证）
router.post("/tokens/export", cookieAuthMiddleware, async (req, res) => {
  const { password } = req.body;

  if (!password || !verifyPassword(password)) {
    return res.status(403).json({ success: false, message: "密码验证失败" });
  }

  try {
    const allTokens = await tokenManager.store.readAll();

    // 导出格式：包含完整的 token 数据
    logger.info("导出所有Token数据");
    const exportData = {
      version: 1,
      exportTime: new Date().toISOString(),
      tokens: allTokens.map((token) => ({
        access_token: token.access_token,
        refresh_token: token.refresh_token,
        expires_in: token.expires_in,
        timestamp: token.timestamp,
        enable: token.enable,
        projectId: token.projectId,
        email: token.email,
        hasQuota: token.hasQuota,
      })),
    };

    res.json({ success: true, data: exportData });
  } catch (error) {
    logger.error("导出Token失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

// 智能查找字段值（不分大小写，包含匹配）
function findFieldByKeyword(obj, keyword) {
  if (!obj || typeof obj !== "object") return undefined;
  const lowerKeyword = keyword.toLowerCase();
  for (const key of Object.keys(obj)) {
    if (key.toLowerCase().includes(lowerKeyword)) {
      return obj[key];
    }
  }
  return undefined;
}

// 智能解析单个 Token 对象
function smartParseToken(rawToken) {
  if (!rawToken || typeof rawToken !== "object") return null;

  // 必需字段：包含 refresh 的认为是 refresh_token，包含 project 的认为是 projectId
  const refresh_token = findFieldByKeyword(rawToken, "refresh");
  const projectId = findFieldByKeyword(rawToken, "project");

  // 必须同时包含这两个字段
  if (!refresh_token || !projectId) return null;

  // 构建标准化的 token 对象
  const token = { refresh_token, projectId };

  // 可选字段自动获取
  const access_token = findFieldByKeyword(rawToken, "access");
  const email =
    findFieldByKeyword(rawToken, "email") ||
    findFieldByKeyword(rawToken, "mail");
  const expires_in = findFieldByKeyword(rawToken, "expire");
  const enable = findFieldByKeyword(rawToken, "enable");
  const timestamp =
    findFieldByKeyword(rawToken, "time") ||
    findFieldByKeyword(rawToken, "stamp");
  const hasQuota = findFieldByKeyword(rawToken, "quota");

  if (access_token) token.access_token = access_token;
  if (email) token.email = email;
  if (expires_in !== undefined) token.expires_in = parseInt(expires_in) || 3599;
  if (enable !== undefined)
    token.enable = enable === true || enable === "true" || enable === 1;
  if (timestamp)
    token.timestamp =
      typeof timestamp === "number" ? timestamp : new Date(timestamp).getTime();
  if (hasQuota !== undefined)
    token.hasQuota = hasQuota === true || hasQuota === "true" || hasQuota === 1;

  return token;
}

// ==================== Gemini CLI Token 导入解析辅助 ====================

function extractGeminiCliImportList(data) {
  // 兼容多种 gcli 导出结构：
  // - { tokens: [...] }
  // - { accounts: [...] }
  // - { data: { tokens/accounts: [...] } }
  // - 直接数组 [...]
  // - 单个凭证对象 { token/refresh_token/project_id/expiry/... }
  if (Array.isArray(data)) return data;
  if (!data || typeof data !== "object") return null;

  const list =
    data.tokens || data.accounts || data.data?.tokens || data.data?.accounts;
  if (Array.isArray(list)) return list;

  const hasRefresh = !!(data.refresh_token || data.refreshToken);
  const hasAccess = !!(data.access_token || data.accessToken || data.token);
  if (hasRefresh || hasAccess) return [data];
  return null;
}

function normalizeTruthyBoolean(value) {
  return value === true || value === "true" || value === 1;
}

function parseGeminiCliEnable(rawToken) {
  // enable/enabled/disabled 兼容
  let enable = findFieldByKeyword(rawToken, "enable");
  if (enable === undefined) enable = findFieldByKeyword(rawToken, "enabled");
  let disabled = findFieldByKeyword(rawToken, "disable");
  if (disabled === undefined)
    disabled = findFieldByKeyword(rawToken, "disabled");
  if (enable === undefined && disabled !== undefined) {
    enable = !normalizeTruthyBoolean(disabled);
  }
  if (enable === undefined) enable = true;
  return normalizeTruthyBoolean(enable);
}

function deriveExpiresInAndTimestamp({ expires_in, expiry, timestamp }) {
  // expires_in / expiry 兼容：
  // - 如果有 expires_in(秒) -> 直接用
  // - 如果只有 expiry(ISO8601) -> 计算剩余秒数，并把 timestamp 设为当前时间
  const nowMs = Date.now();

  let finalExpiresIn = null;
  if (
    expires_in !== undefined &&
    expires_in !== null &&
    String(expires_in).trim() !== ""
  ) {
    const n = parseInt(expires_in, 10);
    if (Number.isFinite(n) && n > 0) finalExpiresIn = n;
  }

  let finalTimestamp = undefined;
  if (finalExpiresIn === null && typeof expiry === "string" && expiry.trim()) {
    const expiryMs = Date.parse(expiry);
    if (Number.isFinite(expiryMs)) {
      finalExpiresIn = Math.max(1, Math.floor((expiryMs - nowMs) / 1000));
      // 用 expiry 推算时，让 timestamp 表示“当前拿到 token 的时间”
      finalTimestamp = nowMs;
    }
  }

  if (finalTimestamp === undefined) {
    if (timestamp) {
      finalTimestamp =
        typeof timestamp === "number"
          ? timestamp
          : new Date(timestamp).getTime();
    } else {
      finalTimestamp = nowMs;
    }
  }

  return {
    expires_in: finalExpiresIn ?? 3599,
    timestamp: finalTimestamp,
  };
}

function smartParseGeminiCliToken(rawToken) {
  if (!rawToken || typeof rawToken !== "object") return null;

  const refresh_token = findFieldByKeyword(rawToken, "refresh");
  if (!refresh_token) return null;

  const token = { refresh_token };

  // gcli 常见字段：token（=access_token）
  const access_token = findFieldByKeyword(rawToken, "access") || rawToken.token;
  const email =
    findFieldByKeyword(rawToken, "email") ||
    findFieldByKeyword(rawToken, "mail");
  const expires_in =
    findFieldByKeyword(rawToken, "expires") ||
    findFieldByKeyword(rawToken, "expire");
  const timestamp =
    findFieldByKeyword(rawToken, "time") ||
    findFieldByKeyword(rawToken, "stamp") ||
    findFieldByKeyword(rawToken, "created");
  const expiry =
    findFieldByKeyword(rawToken, "expiry") ||
    findFieldByKeyword(rawToken, "expiresat");
  const projectId = findFieldByKeyword(rawToken, "project");

  if (access_token) token.access_token = access_token;
  if (email) token.email = email;
  if (projectId) token.projectId = projectId;

  const derived = deriveExpiresInAndTimestamp({
    expires_in,
    expiry,
    timestamp,
  });
  token.expires_in = derived.expires_in;
  token.timestamp = derived.timestamp;
  token.enable = parseGeminiCliEnable(rawToken);

  return token;
}

// 导入 Token（需要密码验证，支持智能字段映射）
router.post("/tokens/import", cookieAuthMiddleware, async (req, res) => {
  const { password, data, mode = "merge" } = req.body;

  if (!password || !verifyPassword(password)) {
    return res.status(403).json({ success: false, message: "密码验证失败" });
  }

  if (!data || !data.tokens || !Array.isArray(data.tokens)) {
    return res
      .status(400)
      .json({ success: false, message: "无效的导入数据格式" });
  }

  try {
    const importTokens = data.tokens;
    let addedCount = 0;
    let skippedCount = 0;
    let updatedCount = 0;

    // 智能解析所有 token
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
      // 替换模式：清空现有数据，导入新数据
      await tokenManager.store.writeAll(parsedTokens);
      addedCount = parsedTokens.length;
    } else {
      // 合并模式：根据 refresh_token 去重
      const existingTokens = await tokenManager.store.readAll();
      const existingRefreshTokens = new Set(
        existingTokens.map((t) => t.refresh_token),
      );

      for (const token of parsedTokens) {
        if (existingRefreshTokens.has(token.refresh_token)) {
          // 更新已存在的 token
          const index = existingTokens.findIndex(
            (t) => t.refresh_token === token.refresh_token,
          );
          if (index !== -1) {
            existingTokens[index] = { ...existingTokens[index], ...token };
            updatedCount++;
          }
        } else {
          // 添加新 token
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

router.post(
  "/oauth/exchange",
  cookieOrPasswordAuthMiddleware,
  async (req, res) => {
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
      } catch (error) {
        return res
          .status(400)
          .json({ success: false, message: "callbackUrl格式无效" });
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
  },
);

router.get("/oauth/url", cookieOrPasswordAuthMiddleware, async (req, res) => {
  const mode = req.query.mode === "geminicli" ? "geminicli" : "antigravity";
  const rawCount = Number.parseInt(String(req.query.count || "1"), 10);
  const count = Math.max(
    1,
    Math.min(100, Number.isInteger(rawCount) ? rawCount : 1),
  );

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

// 获取配置
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

// 更新配置
router.put("/config", cookieAuthMiddleware, (req, res) => {
  try {
    const { env: envUpdates, json: jsonUpdates, password } = req.body;

    if (jsonUpdates?.other) {
      const rawProxyPool = normalizeProxyPoolInput(
        jsonUpdates.other.proxyPool || "",
      );
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

    // 安全检查：如果修改了官方系统提示词，必须验证密码
    if (envUpdates && envUpdates.OFFICIAL_SYSTEM_PROMPT !== undefined) {
      const currentEnv = parseEnvFile(envPath);
      // 正规化换行符后再比较（避免 \r\n 和 \n 不一致导致误判）
      const normalizeNewlines = (str) =>
        (str || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
      const newValue = normalizeNewlines(envUpdates.OFFICIAL_SYSTEM_PROMPT);
      const oldValue = normalizeNewlines(currentEnv.OFFICIAL_SYSTEM_PROMPT);

      // 只有当值真正改变时才检查
      if (newValue !== oldValue) {
        if (!password || !verifyPassword(password)) {
          logger.warn(
            `尝试修改官方系统提示词但密码验证失败 IP: ${getClientIP(req)}`,
          );
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

    // 应用可热更新的运行时配置
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

// 获取禁用代理池
router.get("/proxy-pool/disabled", cookieAuthMiddleware, (req, res) => {
  try {
    const jsonData = getConfigJson();
    const protocol = normalizeProxyProtocol(
      jsonData.other?.proxyProtocol || "http",
    );
    const disabledPoolRaw = normalizeProxyPoolInput(
      jsonData.other?.disabledProxyPool || "",
    );
    const { activeEntries, disabledEntries } = getProxyPoolSummary(
      config.proxy,
    );

    res.json({
      success: true,
      data: {
        proxyProtocol: protocol,
        poolRaw: disabledPoolRaw,
        count: disabledEntries.length,
        activeCount: activeEntries.length,
        entries: parseProxyPool(disabledPoolRaw, protocol).map(
          (entry, index) => ({
            index,
            raw: entry.raw,
            url: entry.url,
            protocol: entry.protocol,
            host: entry.host,
            port: entry.port,
            username: entry.username,
          }),
        ),
      },
    });
  } catch (error) {
    logger.error("获取禁用代理池失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

// 更新禁用代理池
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

// 测试代理池连通性
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
    const concurrencyLimit = normalizeProxyTestConcurrency(
      req.body?.concurrency,
    );

    const entries = parseProxyPool(poolRaw, proxyProtocol);
    if (entries.length === 0) {
      return res.status(400).json({
        success: false,
        message: "启用代理池为空，无法执行测试",
      });
    }

    let workingPoolRaw = poolRaw;

    const results = await runWithConcurrency(
      entries,
      concurrencyLimit,
      async (proxyEntry) =>
        testSingleProxyConnectivity({
          proxyEntry,
          proxyProtocol,
        }),
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
      disabledPoolRaw !==
        normalizeProxyPoolInput(currentJson.other?.disabledProxyPool || "") ||
      workingPoolRaw !==
        normalizeProxyPoolInput(currentJson.other?.proxyPool || "")
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

// 获取轮询策略配置
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

// 更新轮询策略配置
router.put("/rotation", cookieAuthMiddleware, async (req, res) => {
  try {
    const { strategy, requestCount } = req.body;

    // 验证策略值
    const validStrategies = ["round_robin", "quota_exhausted", "request_count"];
    if (strategy && !validStrategies.includes(strategy)) {
      return res.status(400).json({
        success: false,
        message: `无效的策略，可选值: ${validStrategies.join(", ")}`,
      });
    }

    // 更新内存中的配置
    tokenManager.updateRotationConfig(strategy, requestCount);
    geminicliTokenManager.updateRotationConfig(strategy, requestCount);

    // 保存到config.json
    const currentConfig = getConfigJson();
    if (!currentConfig.rotation) currentConfig.rotation = {};
    if (strategy) currentConfig.rotation.strategy = strategy;
    if (requestCount) currentConfig.rotation.requestCount = requestCount;
    saveConfigJson(currentConfig);

    // 重载配置到内存
    reloadConfig();

    await Promise.all([tokenManager.reload(), geminicliTokenManager.reload()]);

    const [antigravityReadyIndices, geminicliReadyIndices] = await Promise.all([
      warmupAntigravityRotationQuotas(),
      warmupGeminiCliRotationQuotas(),
    ]);

    tokenManager.randomizeRotationStart(antigravityReadyIndices);
    geminicliTokenManager.randomizeRotationStart(geminicliReadyIndices);

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

// ==================== 日志管理 API ====================

// 获取日志列表
router.get("/logs", cookieAuthMiddleware, (req, res) => {
  try {
    const { level, search, limit, offset } = req.query;
    const options = {
      level: level || "all",
      search: search || "",
      limit: parseInt(limit) || 100,
      offset: parseInt(offset) || 0,
    };

    const result = logger.getLogs(options);
    res.json({ success: true, data: result });
  } catch (error) {
    logger.error("获取日志失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

// 获取日志统计
router.get("/logs/stats", cookieAuthMiddleware, (req, res) => {
  try {
    const stats = logger.getLogStats();
    res.json({ success: true, data: stats });
  } catch (error) {
    logger.error("获取日志统计失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

// 清空日志
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

// ==================== Token 额度 API ====================

// ==================== Gemini CLI Token 管理 API ====================

// 获取 Gemini CLI Token 列表
router.get("/geminicli/tokens", cookieAuthMiddleware, async (req, res) => {
  try {
    const tokens = await geminicliTokenManager.getTokenList();
    res.json({ success: true, data: tokens });
  } catch (error) {
    logger.error("[GeminiCLI] 获取Token列表失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

// 添加 Gemini CLI Token
router.post("/geminicli/tokens", cookieAuthMiddleware, async (req, res) => {
  const { access_token, refresh_token, expires_in, timestamp, enable, email } =
    req.body;
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

// 更新 Gemini CLI Token
router.put(
  "/geminicli/tokens/:tokenId",
  cookieAuthMiddleware,
  async (req, res) => {
    const { tokenId } = req.params;
    const updates = req.body;

    try {
      // 如果是从禁用状态启用，且请求中包含 enableWithTest 标志，则先测试可用性
      if (updates.enable === true && updates.enableWithTest) {
        delete updates.enableWithTest;
        const result = await geminicliTokenManager.enableTokenById(tokenId);
        if (result.success) {
          logger.info(`[GeminiCLI] 启用Token(已验证): ${tokenId}`);
        } else {
          logger.warn(
            `[GeminiCLI] 启用Token验证失败: ${tokenId} - ${result.message}`,
          );
        }
        res.json(result);
      } else {
        const result = await geminicliTokenManager.updateTokenById(
          tokenId,
          updates,
        );
        logger.info(`[GeminiCLI] 更新Token: ${tokenId}`);
        res.json(result);
      }
    } catch (error) {
      logger.error("[GeminiCLI] 更新Token失败:", error.message);
      res.status(500).json({ success: false, message: error.message });
    }
  },
);

// 删除 Gemini CLI Token
router.delete(
  "/geminicli/tokens/:tokenId",
  cookieAuthMiddleware,
  async (req, res) => {
    const { tokenId } = req.params;
    try {
      const result = await geminicliTokenManager.deleteTokenById(tokenId);
      logger.info(`[GeminiCLI] 删除Token: ${tokenId}`);
      res.json(result);
    } catch (error) {
      logger.error("[GeminiCLI] 删除Token失败:", error.message);
      res.status(500).json({ success: false, message: error.message });
    }
  },
);

// 热重载 Gemini CLI Token
router.post(
  "/geminicli/tokens/reload",
  cookieAuthMiddleware,
  async (req, res) => {
    try {
      await geminicliTokenManager.reload();
      logger.info("[GeminiCLI] 手动触发Token热重载");
      res.json({ success: true, message: "Gemini CLI Token已热重载" });
    } catch (error) {
      logger.error("[GeminiCLI] 热重载失败:", error.message);
      res.status(500).json({ success: false, message: error.message });
    }
  },
);

router.post(
  "/geminicli/tokens/batch",
  cookieAuthMiddleware,
  async (req, res) => {
    const action = String(req.body?.action || "").trim();
    const tokenIds = normalizeBatchTokenIds(req.body?.tokenIds);

    if (!action) {
      return res.status(400).json({ success: false, message: "action必填" });
    }

    if (tokenIds.length === 0) {
      return res
        .status(400)
        .json({ success: false, message: "tokenIds不能为空" });
    }

    try {
      if (action === "export") {
        const { password } = req.body || {};
        if (!password || !verifyPassword(password)) {
          return res
            .status(403)
            .json({ success: false, message: "密码验证失败" });
        }

        const { results, tokens } = await buildBatchExportResults(
          tokenIds,
          (tokenId) => geminicliTokenManager.findTokenById(tokenId),
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
            const projectResult =
              await geminicliTokenManager.fetchProjectIdForToken(tokenId);
            result = {
              success: true,
              message: "Project ID获取成功",
              projectId: projectResult.projectId,
              tier: projectResult.tier || null,
            };
          } else if (action === "refresh_token") {
            const refreshResult =
              await geminicliTokenManager.refreshTokenById(tokenId);
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
            ...(result.modelCount !== undefined
              ? { modelCount: result.modelCount }
              : {}),
          });
        } catch (error) {
          results.push({
            tokenId,
            success: false,
            message: error.message || "操作失败",
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
  },
);

// 刷新指定 Gemini CLI Token
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
      logger.error("[GeminiCLI] 刷新Token失败:", error.message);
      const status = error.statusCode || 500;
      res.status(status).json({ success: false, message: error.message });
    }
  },
);

// 手动获取指定 Gemini CLI Token 的 Project ID
router.post(
  "/geminicli/tokens/:tokenId/fetch-project-id",
  cookieAuthMiddleware,
  async (req, res) => {
    const { tokenId } = req.params;
    try {
      const result =
        await geminicliTokenManager.fetchProjectIdForToken(tokenId);
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
      logger.error("[GeminiCLI] 获取ProjectId失败:", error.message);
      const status = error.statusCode || 500;
      res.status(status).json({ success: false, message: error.message });
    }
  },
);

// 批量获取所有已启用 Gemini CLI Token 的 Project ID
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
      logger.error("[GeminiCLI] 批量获取ProjectId失败:", error.message);
      const status = error.statusCode || error.status || 500;
      res.status(status).json({ success: false, message: error.message });
    }
  },
);

// 获取 Gemini CLI Token 的额度信息
router.get(
  "/geminicli/tokens/:tokenId/quotas",
  cookieAuthMiddleware,
  async (req, res) => {
    try {
      const { tokenId } = req.params;
      const forceRefresh = req.query.refresh === "true";

      let tokenData = await geminicliTokenManager.findTokenById(tokenId);
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
        `[GeminiCLI] 获取额度失败 | status=${status} | tokenId=${tokenId} | email=${tokenData?.email || ""} | projectId=${tokenData?.projectId || ""} | response=${responseData || "(empty)"} | headers=${responseHeaders || "(empty)"} | message=${error.message}`,
      );
      res.status(status).json({ success: false, message: error.message });
    }
  },
);

// 导出 Gemini CLI Token（需要密码验证）
router.post(
  "/geminicli/tokens/export",
  cookieAuthMiddleware,
  async (req, res) => {
    const { password } = req.body;

    if (!password || !verifyPassword(password)) {
      return res.status(403).json({ success: false, message: "密码验证失败" });
    }

    try {
      const allTokens = await geminicliTokenManager.store.readAll();

      logger.info("[GeminiCLI] 导出所有Token数据");
      const exportData = {
        version: 1,
        exportTime: new Date().toISOString(),
        tokens: allTokens.map((token) => ({
          access_token: token.access_token,
          refresh_token: token.refresh_token,
          expires_in: token.expires_in,
          timestamp: token.timestamp,
          enable: token.enable,
          email: token.email,
          projectId: token.projectId,
          tier: token.tier,
        })),
      };

      res.json({ success: true, data: exportData });
    } catch (error) {
      logger.error("[GeminiCLI] 导出Token失败:", error.message);
      res.status(500).json({ success: false, message: error.message });
    }
  },
);

// 导入 Gemini CLI Token（需要密码验证）
router.post(
  "/geminicli/tokens/import",
  cookieAuthMiddleware,
  async (req, res) => {
    const { password, data, mode = "merge" } = req.body;

    if (!password || !verifyPassword(password)) {
      return res.status(403).json({ success: false, message: "密码验证失败" });
    }

    const importList = extractGeminiCliImportList(data);

    if (!Array.isArray(importList)) {
      return res
        .status(400)
        .json({ success: false, message: "无效的导入数据格式" });
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
          existingTokens.map((t) => t.refresh_token),
        );

        for (const token of parsedTokens) {
          if (existingRefreshTokens.has(token.refresh_token)) {
            const index = existingTokens.findIndex(
              (t) => t.refresh_token === token.refresh_token,
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
  },
);

// ==================== IP 封禁管理 API ====================

// 获取封禁列表
router.get("/blocked-ips", cookieAuthMiddleware, async (req, res) => {
  try {
    const list = await ipBlockManager.listBlocked();
    res.json({ success: true, data: list });
  } catch (error) {
    logger.error("获取封禁列表失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

// 解除IP封禁
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

// 获取安全配置
router.get("/security-config", cookieAuthMiddleware, async (req, res) => {
  try {
    const config = ipBlockManager.getConfig();
    res.json({ success: true, data: config });
  } catch (error) {
    logger.error("获取安全配置失败:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

// 更新安全配置
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

// 添加白名单IP
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

// 移除白名单IP
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

// ==================== Token 额度 API ====================

// 获取指定Token的模型额度（使用 tokenId）
router.get(
  "/tokens/:tokenId/quotas",
  cookieAuthMiddleware,
  async (req, res) => {
    try {
      const { tokenId } = req.params;
      const forceRefresh = req.query.refresh === "true";

      // 通过 tokenId 查找完整的 token 数据
      let tokenData = await tokenManager.findTokenById(tokenId);

      if (!tokenData) {
        return res.status(404).json({ success: false, message: "Token不存在" });
      }

      // 检查 token 是否禁用
      const isDisabled = tokenData.enable === false;

      // 使用 tokenId 作为缓存键，优先获取缓存数据
      let quotaData = quotaManager.getQuota(tokenId);

      // 禁用的 token 只返回缓存数据，不刷新也不获取新数据
      if (isDisabled) {
        if (!quotaData) {
          // 没有缓存数据，返回空数据
          quotaData = { lastUpdated: null, models: {} };
        }
      } else {
        // 启用的 token 正常处理
        // 检查token是否过期，如果过期则刷新
        if (tokenManager.isExpired(tokenData)) {
          try {
            tokenData = await tokenManager.refreshToken(tokenData);
          } catch (error) {
            logger.error("刷新token失败:", error.message);
            // 使用 400 而不是 401，避免前端误认为 JWT 登录过期
            return res.status(400).json({
              success: false,
              message: "Google Token已过期且刷新失败，请重新登录Google账号",
            });
          }
        }

        // 强制刷新时清除缓存
        if (forceRefresh) {
          quotaData = null;
        }

        if (!quotaData) {
          // 缓存未命中或强制刷新，从API获取
          const quotas = await getModelsWithQuotas(tokenData);
          quotaManager.updateQuota(tokenId, quotas);
          // 从缓存中获取完整数据（包含 requestCounts），而不是构造不完整的对象
          quotaData = quotaManager.getQuota(tokenId) || {
            lastUpdated: Date.now(),
            models: quotas,
            requestCounts: {},
          };
        }
      }

      // 转换时间为北京时间
      const modelsWithBeijingTime = {};
      Object.entries(quotaData.models).forEach(([modelId, quota]) => {
        modelsWithBeijingTime[modelId] = {
          remaining: quota.r,
          resetTime: quotaManager.convertToBeijingTime(quota.t),
          resetTimeRaw: quota.t,
        };
      });

      // 获取请求计数
      const requestCounts = quotaData.requestCounts || {};

      res.json({
        success: true,
        data: {
          lastUpdated: quotaData.lastUpdated,
          models: modelsWithBeijingTime,
          requestCounts, // 返回请求计数供前端计算预估
        },
      });
    } catch (error) {
      logger.error("获取额度失败:", error.message);
      res.status(500).json({ success: false, message: error.message });
    }
  },
);

export default router;
