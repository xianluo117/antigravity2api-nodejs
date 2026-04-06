/**
 * 模型分组工具模块
 * 统一管理模型到组的映射逻辑，供 quota_manager 和 token_cooldown_manager 共用
 */

/**
 * 默认（Antigravity）模型组列表
 * - claude: Claude 系列模型
 * - gemini: Gemini 系列模型
 * - banana: gemini-3.1-flash-image 图片生成模型
 * - other: 其他模型
 */
export const MODEL_GROUPS = ["claude", "gemini", "banana", "other"];

/**
 * Gemini CLI 模型组列表
 * - flash: Flash 系列模型
 * - pro: Pro 系列模型
 * - other: 其他模型
 */
export const GEMINICLI_MODEL_GROUPS = ["flash", "pro", "other"];

export const MODEL_GROUPING_MODES = Object.freeze({
  DEFAULT: "default",
  GEMINICLI: "geminicli",
});

function getDefaultGroupKey(lower) {
  if (lower.includes("claude")) return "claude";
  // banana 必须在 gemini 之前检查，因为它包含 'gemini' 字符串
  if (lower.includes("gemini-3.1-flash-image")) return "banana";
  if (lower.includes("gemini") || lower.includes("publishers/google/"))
    return "gemini";
  return "other";
}

function getGeminiCliGroupKey(lower) {
  if (lower.includes("flash")) return "flash";
  if (lower.includes("pro")) return "pro";
  return "other";
}

/**
 * 获取模型所属的组 key
 * @param {string} modelId - 模型 ID
 * @param {string} [groupingMode='default'] - 分组模式
 * @returns {string} 组 key
 */
export function getGroupKey(
  modelId,
  groupingMode = MODEL_GROUPING_MODES.DEFAULT,
) {
  if (!modelId) return "other";
  const lower = modelId.toLowerCase();

  if (groupingMode === MODEL_GROUPING_MODES.GEMINICLI) {
    return getGeminiCliGroupKey(lower);
  }

  return getDefaultGroupKey(lower);
}
