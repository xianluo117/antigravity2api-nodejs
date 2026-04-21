import tokenCooldownManager from "../auth/token_cooldown_manager.js";

const CORE_GROUPS_BY_MODE = {
  default: {
    claude: "claude-3-5-sonnet-20241022",
    gemini: "gemini-2.0-flash-exp",
    banana: "gemini-3.1-flash-image",
  },
  geminicli: {
    flash: "gemini-2.5-flash",
    pro: "gemini-2.5-pro",
  },
};

function getCoreGroups(groupingMode = "default") {
  return CORE_GROUPS_BY_MODE[groupingMode] || CORE_GROUPS_BY_MODE.default;
}

/**
 * 检查 token 是否还有其他可用的模型组
 * @param {string} tokenId - Token ID
 * @param {string} [groupingMode="default"] - 分组模式
 * @returns {boolean}
 */
export function hasOtherAvailableModelGroups(
  tokenId,
  groupingMode = "default",
) {
  const coreGroups = getCoreGroups(groupingMode);
  return Object.values(coreGroups).some((modelId) =>
    tokenCooldownManager.isAvailable(tokenId, modelId, groupingMode),
  );
}

/**
 * 获取 token 当前可用的模型组列表
 * @param {string} tokenId - Token ID
 * @param {string} [groupingMode="default"] - 分组模式
 * @returns {string[]}
 */
export function getAvailableModelGroups(tokenId, groupingMode = "default") {
  const coreGroups = getCoreGroups(groupingMode);
  return Object.entries(coreGroups)
    .filter(([, modelId]) =>
      tokenCooldownManager.isAvailable(tokenId, modelId, groupingMode),
    )
    .map(([groupKey]) => groupKey);
}
