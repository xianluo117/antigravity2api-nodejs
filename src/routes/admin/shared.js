import geminicliTokenManager from "../../auth/geminicli_token_manager.js";
import { verifyToken } from "../../auth/jwt.js";
import quotaManager from "../../auth/quota_manager.js";
import tokenManager from "../../auth/token_manager.js";
import config from "../../config/config.js";
import { getModelsWithQuotas } from "../../api/client.js";
import { getGeminiCliQuotas } from "../../api/geminicli_client.js";

export const COOKIE_OPTIONS = {
  httpOnly: true,
  sameSite: "strict",
  maxAge: 24 * 60 * 60 * 1000,
};

const CODE_ASSIST_AUTH_TARGET_PREFIX =
  "https://developers.google.com/gemini-code-assist/auth/";
const URL_MATCH_REGEX = /https?:\/\/[^\s"'<>]+/gi;

export function getRequestPassword(req) {
  if (typeof req.body?.password === "string") {
    return req.body.password;
  }
  if (typeof req.query?.password === "string") {
    return req.query.password;
  }
  return null;
}

export function getRequestEmail(req) {
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

export function getRequestMode(req, defaultMode = "auto") {
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

export function buildForbiddenGoogleAccountList(
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

export function normalizeBatchTokenIds(tokenIds) {
  if (!Array.isArray(tokenIds)) return [];
  return Array.from(
    new Set(
      tokenIds.map((tokenId) => String(tokenId || "").trim()).filter(Boolean),
    ),
  );
}

export function buildBatchResponsePayload(action, results, extra = {}) {
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

export async function refreshAntigravityQuotaByTokenId(tokenId) {
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

export async function refreshGeminiCliQuotaByTokenId(tokenId) {
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

export function buildAntigravityExportData(tokens) {
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
      credits: token.credits,
    })),
  };
}

export function buildGeminiCliExportData(tokens) {
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

export async function buildBatchExportResults(tokenIds, findTokenById) {
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

export function findTokenByEmail(tokens, email) {
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

export function verifyPassword(password) {
  return password === config.admin.password;
}

export function cookieAuthMiddleware(req, res, next) {
  let token = req.cookies?.authToken;

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
  } catch {
    res.clearCookie("authToken", {
      ...COOKIE_OPTIONS,
      secure: req.secure || process.env.NODE_ENV === "production",
    });
    return res.status(401).json({ error: "Invalid token" });
  }
}

export function cookieOrPasswordAuthMiddleware(req, res, next) {
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
    } catch {
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
}

export function getClientIP(req) {
  return (
    req.headers["x-forwarded-for"]?.split(",")[0]?.trim() ||
    req.headers["x-real-ip"] ||
    req.connection?.remoteAddress ||
    req.ip ||
    "unknown"
  );
}

export function findFieldByKeyword(obj, keyword) {
  if (!obj || typeof obj !== "object") return undefined;
  const lowerKeyword = keyword.toLowerCase();
  for (const key of Object.keys(obj)) {
    if (key.toLowerCase().includes(lowerKeyword)) {
      return obj[key];
    }
  }
  return undefined;
}

export function smartParseToken(rawToken) {
  if (!rawToken || typeof rawToken !== "object") return null;

  const refresh_token = findFieldByKeyword(rawToken, "refresh");
  const projectId = findFieldByKeyword(rawToken, "project");

  if (!refresh_token || !projectId) return null;

  const token = { refresh_token, projectId };

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
  const sub =
    findFieldByKeyword(rawToken, "sub") || findFieldByKeyword(rawToken, "tier");
  const credits = findFieldByKeyword(rawToken, "credit");

  if (access_token) token.access_token = access_token;
  if (email) token.email = email;
  if (expires_in !== undefined) token.expires_in = parseInt(expires_in, 10) || 3599;
  if (enable !== undefined) {
    token.enable = enable === true || enable === "true" || enable === 1;
  }
  if (timestamp) {
    token.timestamp =
      typeof timestamp === "number" ? timestamp : new Date(timestamp).getTime();
  }
  if (hasQuota !== undefined) {
    token.hasQuota = hasQuota === true || hasQuota === "true" || hasQuota === 1;
  }
  if (sub) {
    token.sub = String(sub).trim();
  }
  if (credits !== undefined && credits !== null && credits !== "") {
    const parsedCredits = Number.parseFloat(credits);
    if (Number.isFinite(parsedCredits)) {
      token.credits = parsedCredits;
    }
  }

  return token;
}

export function extractGeminiCliImportList(data) {
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

export function normalizeTruthyBoolean(value) {
  return value === true || value === "true" || value === 1;
}

export function parseGeminiCliEnable(rawToken) {
  let enable = findFieldByKeyword(rawToken, "enable");
  if (enable === undefined) enable = findFieldByKeyword(rawToken, "enabled");
  let disabled = findFieldByKeyword(rawToken, "disable");
  if (disabled === undefined) {
    disabled = findFieldByKeyword(rawToken, "disabled");
  }
  if (enable === undefined && disabled !== undefined) {
    enable = !normalizeTruthyBoolean(disabled);
  }
  if (enable === undefined) enable = true;
  return normalizeTruthyBoolean(enable);
}

export function deriveExpiresInAndTimestamp({ expires_in, expiry, timestamp }) {
  const nowMs = Date.now();

  let finalExpiresIn = null;
  if (
    expires_in !== undefined &&
    expires_in !== null &&
    String(expires_in).trim() !== ""
  ) {
    const value = parseInt(expires_in, 10);
    if (Number.isFinite(value) && value > 0) finalExpiresIn = value;
  }

  let finalTimestamp = undefined;
  if (finalExpiresIn === null && typeof expiry === "string" && expiry.trim()) {
    const expiryMs = Date.parse(expiry);
    if (Number.isFinite(expiryMs)) {
      finalExpiresIn = Math.max(1, Math.floor((expiryMs - nowMs) / 1000));
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

export function smartParseGeminiCliToken(rawToken) {
  if (!rawToken || typeof rawToken !== "object") return null;

  const refresh_token = findFieldByKeyword(rawToken, "refresh");
  if (!refresh_token) return null;

  const token = { refresh_token };

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
