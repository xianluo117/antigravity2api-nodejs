import crypto from "crypto";
import fs from "fs";
import path from "path";
import config, {
  getConfigJson,
  getProxyConfig,
  saveConfigJson,
} from "../config/config.js";
import logger from "./logger.js";
import { getDataDir } from "./paths.js";

const PROXY_ROTATION_STATE_FILE = "proxy_rotation.json";

export function normalizeProxyProtocol(protocol = "http") {
  const normalized = String(protocol || "http")
    .trim()
    .toLowerCase();

  if (
    normalized === "socket5" ||
    normalized === "socks" ||
    normalized === "socks5"
  ) {
    return "socks5";
  }

  if (normalized === "https") {
    return "https";
  }

  return "http";
}

function normalizeLines(value = "") {
  return String(value ?? "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n");
}

export function normalizeProxyPoolInput(poolRaw = "") {
  return splitPoolLines(poolRaw).join("\n");
}

function splitPoolLines(poolRaw = "") {
  return normalizeLines(poolRaw)
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function buildProxyUrl({ protocol, host, port, username = "", password = "" }) {
  const auth = username
    ? `${encodeURIComponent(username)}:${encodeURIComponent(password || "")}@`
    : "";

  return `${protocol}://${auth}${host}:${port}`;
}

function getProxyIdentity(proxyEntry) {
  if (!proxyEntry) return "";
  if (proxyEntry.normalizedUrl) return proxyEntry.normalizedUrl;

  const protocol = normalizeProxyProtocol(proxyEntry.protocol || "http");
  const host = String(proxyEntry.host || "").trim();
  const port = Number.parseInt(proxyEntry.port, 10);
  if (!host || !Number.isInteger(port) || port <= 0) {
    return String(proxyEntry.url || proxyEntry.raw || "").trim();
  }

  return buildProxyUrl({
    protocol,
    host,
    port,
    username: proxyEntry.username || "",
    password: proxyEntry.password || "",
  });
}

function dedupeProxyEntries(entries = []) {
  const seen = new Set();
  return entries.filter((entry) => {
    const identity = getProxyIdentity(entry);
    if (!identity || seen.has(identity)) {
      return false;
    }
    seen.add(identity);
    return true;
  });
}

function parseProxyUrl(proxyUrl) {
  const rawValue = String(proxyUrl).trim();
  if (!rawValue.includes("://")) {
    return null;
  }

  try {
    const parsedUrl = new URL(rawValue);
    const protocol = normalizeProxyProtocol(
      parsedUrl.protocol.replace(":", ""),
    );

    return {
      raw: rawValue,
      protocol,
      host: parsedUrl.hostname,
      port: Number.parseInt(parsedUrl.port, 10) || undefined,
      username: decodeURIComponent(parsedUrl.username || ""),
      password: decodeURIComponent(parsedUrl.password || ""),
      url: rawValue,
      normalizedUrl:
        parsedUrl.hostname && Number.parseInt(parsedUrl.port, 10) > 0
          ? buildProxyUrl({
              protocol,
              host: parsedUrl.hostname,
              port: Number.parseInt(parsedUrl.port, 10),
              username: decodeURIComponent(parsedUrl.username || ""),
              password: decodeURIComponent(parsedUrl.password || ""),
            })
          : rawValue,
    };
  } catch {
    return null;
  }
}

function parseProxyPoolLine(line, protocol) {
  const trimmed = line.trim();
  if (!trimmed) return null;

  const urlProxy = parseProxyUrl(trimmed);
  if (urlProxy) {
    return urlProxy;
  }

  if (trimmed.includes("@")) {
    const [authPart, hostPart] = trimmed.split("@");
    const authSegments = authPart.split(":");
    const hostSegments = hostPart.split(":");

    if (authSegments.length >= 2 && hostSegments.length >= 2) {
      const username = authSegments.shift() || "";
      const password = authSegments.join(":");
      const host = hostSegments.shift() || "";
      const port = Number.parseInt(hostSegments.join(":"), 10);

      if (!host || !username || !Number.isInteger(port) || port <= 0) {
        throw new Error(`代理格式无效: ${trimmed}`);
      }

      return {
        raw: trimmed,
        protocol,
        host,
        port,
        username,
        password,
        url: buildProxyUrl({ protocol, host, port, username, password }),
        normalizedUrl: buildProxyUrl({
          protocol,
          host,
          port,
          username,
          password,
        }),
      };
    }
  }

  const segments = trimmed.split(":");
  if (segments.length < 2) {
    throw new Error(`代理格式无效: ${trimmed}`);
  }

  const [host, portRaw, usernameRaw = "", ...passwordParts] = segments;
  const port = Number.parseInt(portRaw, 10);
  if (!host || !Number.isInteger(port) || port <= 0) {
    throw new Error(`代理格式无效: ${trimmed}`);
  }

  const username = usernameRaw || "";
  const password = passwordParts.join(":");

  return {
    raw: trimmed,
    protocol,
    host,
    port,
    username,
    password,
    url: buildProxyUrl({ protocol, host, port, username, password }),
    normalizedUrl: buildProxyUrl({
      protocol,
      host,
      port,
      username,
      password,
    }),
  };
}

export function parseProxyPool(poolRaw = "", protocol = "http") {
  const normalizedProtocol = normalizeProxyProtocol(protocol);
  return splitPoolLines(poolRaw)
    .map((line) => parseProxyPoolLine(line, normalizedProtocol))
    .filter(Boolean);
}

function getRotationStatePath() {
  const dataDir = getDataDir();
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  return path.join(dataDir, PROXY_ROTATION_STATE_FILE);
}

function loadRotationState() {
  try {
    const statePath = getRotationStatePath();
    if (!fs.existsSync(statePath)) {
      return { poolKey: null, nextIndex: 0 };
    }

    const raw = fs.readFileSync(statePath, "utf8");
    const parsed = JSON.parse(raw);

    return {
      poolKey: typeof parsed.poolKey === "string" ? parsed.poolKey : null,
      nextIndex: Number.isInteger(parsed.nextIndex) ? parsed.nextIndex : 0,
    };
  } catch (error) {
    logger.warn(`读取代理轮询状态失败: ${error.message}`);
    return { poolKey: null, nextIndex: 0 };
  }
}

function saveRotationState({ poolKey, nextIndex, updatedAt = Date.now() }) {
  try {
    const statePath = getRotationStatePath();
    fs.writeFileSync(
      statePath,
      JSON.stringify({ poolKey, nextIndex, updatedAt }, null, 2),
      "utf8",
    );
  } catch (error) {
    logger.warn(`保存代理轮询状态失败: ${error.message}`);
  }
}

function buildPoolKey(protocol, poolRaw) {
  return crypto
    .createHash("sha1")
    .update(`${normalizeProxyProtocol(protocol)}\n${normalizeLines(poolRaw)}`)
    .digest("hex");
}

function resolveProxyEntries(proxyConfig) {
  if (!proxyConfig) return [];

  if (typeof proxyConfig === "string") {
    const parsedUrl = parseProxyUrl(proxyConfig);
    if (parsedUrl) return [parsedUrl];
    return parseProxyPool(proxyConfig, config.proxy?.protocol || "http");
  }

  if (typeof proxyConfig !== "object" || proxyConfig.enabled === false) {
    return [];
  }

  if (proxyConfig.poolRaw) {
    const entries = parseProxyPool(proxyConfig.poolRaw, proxyConfig.protocol);
    const disabledEntries = proxyConfig.disabledPoolRaw
      ? parseProxyPool(proxyConfig.disabledPoolRaw, proxyConfig.protocol)
      : [];

    if (disabledEntries.length === 0) {
      return entries;
    }

    const disabledSet = new Set(
      disabledEntries.map((entry) => getProxyIdentity(entry)),
    );
    return entries.filter((entry) => !disabledSet.has(getProxyIdentity(entry)));
  }

  if (proxyConfig.url) {
    const parsedUrl = parseProxyUrl(proxyConfig.url);
    if (parsedUrl) return [parsedUrl];
    return parseProxyPool(
      proxyConfig.url,
      proxyConfig.protocol || config.proxy?.protocol || "http",
    );
  }

  return [];
}

export function getNextProxyConfig(proxyOverride = undefined) {
  const effectiveProxy =
    proxyOverride !== undefined ? proxyOverride : config.proxy;
  const entries = resolveProxyEntries(effectiveProxy);

  if (entries.length === 0) {
    return null;
  }

  if (entries.length === 1) {
    return {
      ...entries[0],
      index: 0,
      poolSize: 1,
      isPool: false,
    };
  }

  const protocol = normalizeProxyProtocol(
    typeof effectiveProxy === "object"
      ? effectiveProxy.protocol
      : entries[0].protocol,
  );
  const poolRaw =
    typeof effectiveProxy === "object" && effectiveProxy.poolRaw
      ? effectiveProxy.poolRaw
      : entries.map((entry) => entry.raw).join("\n");

  const poolKey = buildPoolKey(protocol, poolRaw);
  const state = loadRotationState();
  const nextIndex = state.poolKey === poolKey ? state.nextIndex : 0;
  const index =
    ((nextIndex % entries.length) + entries.length) % entries.length;
  const selected = entries[index];

  saveRotationState({
    poolKey,
    nextIndex: (index + 1) % entries.length,
  });

  return {
    ...selected,
    index,
    poolSize: entries.length,
    isPool: true,
  };
}

export function getProxyPoolSummary(proxyConfig = undefined) {
  const effectiveProxy = proxyConfig !== undefined ? proxyConfig : config.proxy;
  const protocol = normalizeProxyProtocol(
    effectiveProxy?.protocol || config.proxy?.protocol || "http",
  );

  return {
    protocol,
    activeEntries:
      effectiveProxy &&
      typeof effectiveProxy === "object" &&
      effectiveProxy.poolRaw
        ? parseProxyPool(effectiveProxy.poolRaw, protocol)
        : [],
    disabledEntries:
      effectiveProxy &&
      typeof effectiveProxy === "object" &&
      effectiveProxy.disabledPoolRaw
        ? parseProxyPool(effectiveProxy.disabledPoolRaw, protocol)
        : [],
  };
}

export function disableProxyInPool(proxyEntry, reason = "") {
  try {
    const targetIdentity = getProxyIdentity(proxyEntry);
    if (!targetIdentity) {
      return { changed: false, reason: "missing_proxy_identity" };
    }

    const currentJson = getConfigJson();
    const protocol = normalizeProxyProtocol(
      currentJson.other?.proxyProtocol ||
        proxyEntry?.protocol ||
        config.proxy?.protocol ||
        "http",
    );
    const activeEntries = parseProxyPool(
      normalizeProxyPoolInput(currentJson.other?.proxyPool || ""),
      protocol,
    );
    const disabledEntries = parseProxyPool(
      normalizeProxyPoolInput(currentJson.other?.disabledProxyPool || ""),
      protocol,
    );

    const targetEntry = activeEntries.find(
      (entry) => getProxyIdentity(entry) === targetIdentity,
    );

    if (!targetEntry) {
      const alreadyDisabled = disabledEntries.some(
        (entry) => getProxyIdentity(entry) === targetIdentity,
      );
      return {
        changed: false,
        alreadyDisabled,
        reason: alreadyDisabled
          ? "already_disabled"
          : "not_found_in_active_pool",
      };
    }

    const nextActiveEntries = activeEntries.filter(
      (entry) => getProxyIdentity(entry) !== targetIdentity,
    );
    const nextDisabledEntries = dedupeProxyEntries([
      ...disabledEntries,
      targetEntry,
    ]);

    saveConfigJson({
      other: {
        proxyProtocol: protocol,
        proxyPool: nextActiveEntries.map((entry) => entry.raw).join("\n"),
        disabledProxyPool: nextDisabledEntries
          .map((entry) => entry.raw)
          .join("\n"),
      },
    });

    config.proxy = getProxyConfig(getConfigJson());

    logger.warn(
      `[ProxyPool] 代理已移入禁用池: ${targetEntry.url}${reason ? `，原因: ${reason}` : ""}`,
    );

    return {
      changed: true,
      entry: targetEntry,
      activeCount: nextActiveEntries.length,
      disabledCount: nextDisabledEntries.length,
    };
  } catch (error) {
    logger.warn(`[ProxyPool] 自动禁用代理失败: ${error.message}`);
    return {
      changed: false,
      reason: "disable_failed",
      error: error.message,
    };
  }
}

export function formatProxyRequestInfo(proxyConfig, targetUrl = "") {
  if (!proxyConfig) return "";

  const proxyLabel = proxyConfig.isPool
    ? `代理池[${(proxyConfig.index ?? 0) + 1}/${proxyConfig.poolSize || 1}]`
    : "代理";
  const targetLabel = targetUrl ? ` -> ${targetUrl}` : "";

  return `${proxyLabel} ${proxyConfig.url}${targetLabel}`;
}
