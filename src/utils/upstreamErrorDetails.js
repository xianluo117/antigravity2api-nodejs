const TOKEN_FIELD_PATTERN = /(^|[_-])(access|refresh)?token($|[_-])|authorization/i;
const TOKEN_VALUE_PATTERN = /\b(?:ya29\.[A-Za-z0-9._-]+|1\/\/[A-Za-z0-9._-]+|Bearer\s+[A-Za-z0-9._-]{12,})\b/g;

function sanitizeString(value) {
  return String(value)
    .replace(TOKEN_VALUE_PATTERN, (match) => {
      if (/^Bearer\s+/i.test(match)) {
        return "Bearer [REDACTED_TOKEN]";
      }
      return "[REDACTED_TOKEN]";
    })
    .replace(/("(?:access_token|refresh_token|id_token|token|authorization)"\s*:\s*")([^"]+)(")/gi, "$1[REDACTED_TOKEN]$3");
}

function sanitizeValue(value, depth = 0) {
  if (value === null || value === undefined) return value;
  if (typeof value === "string") return sanitizeString(value);
  if (typeof value !== "object") return value;
  if (depth >= 6) return "[Object]";
  if (Array.isArray(value)) {
    return value.map((item) => sanitizeValue(item, depth + 1));
  }

  const result = {};
  for (const [key, item] of Object.entries(value)) {
    result[key] = TOKEN_FIELD_PATTERN.test(key)
      ? "[REDACTED_TOKEN]"
      : sanitizeValue(item, depth + 1);
  }
  return result;
}

function stringifyPart(value) {
  if (value === null || value === undefined || value === "") return "";
  if (typeof value === "string") return sanitizeString(value);
  try {
    return sanitizeString(JSON.stringify(sanitizeValue(value)));
  } catch {
    return sanitizeString(String(value));
  }
}

function pushPart(parts, label, value) {
  const text = stringifyPart(value);
  if (text) {
    parts.push(`${label}: ${text}`);
  }
}

function getStatus(error) {
  return error?.response?.status || error?.status || error?.statusCode || 500;
}

export function extractUpstreamErrorDetails(error, fallbackMessage = "请求失败") {
  const status = getStatus(error);
  const statusText = error?.response?.statusText;
  const data = error?.response?.data;
  const upstreamError = data?.error && typeof data.error === "object" ? data.error : null;
  const parts = [];

  if (status) parts.push(`HTTP ${status}${statusText ? ` ${statusText}` : ""}`);

  if (upstreamError) {
    pushPart(parts, "message", upstreamError.message);
    pushPart(parts, "status", upstreamError.status);
    pushPart(parts, "details", upstreamError.details);
  } else if (typeof data === "string") {
    pushPart(parts, "body", data);
  } else if (data && typeof data === "object") {
    pushPart(parts, "body", data);
  }

  if (parts.length <= (status ? 1 : 0)) {
    pushPart(parts, "message", error?.message || fallbackMessage);
  }

  const message = parts.join(" | ") || sanitizeString(fallbackMessage);
  return {
    status,
    statusText: statusText || null,
    data: sanitizeValue(data),
    message,
  };
}

export function getUpstreamErrorMessage(error, fallbackMessage = "请求失败") {
  return extractUpstreamErrorDetails(error, fallbackMessage).message;
}

export function getUpstreamErrorStatus(error, fallbackStatus = 500) {
  return error?.response?.status || error?.status || error?.statusCode || fallbackStatus;
}

export function sanitizeErrorMessage(message) {
  return sanitizeString(message || "");
}
