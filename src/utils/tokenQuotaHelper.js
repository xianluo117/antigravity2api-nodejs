/**
 * Token 配额检查辅助函数
 * 用于检查 token 的模型组可用性，支持 429 错误时的级联配额管理
 *
 * 当某个模型组被 429 冷却后，检查该 token 是否还有其他可用模型组。
 * 如果所有核心模型组都被禁用，则标记整个 token 配额耗尽。
 */
import tokenCooldownManager from '../auth/token_cooldown_manager.js';
import { MODEL_GROUPS } from './modelGroups.js';

// 核心模型组及其代表性模型（用于 isAvailable 检查）
const CORE_GROUP_REPRESENTATIVES = {
  claude: 'claude-3-5-sonnet-20241022',
  gemini: 'gemini-2.0-flash-exp',
  banana: 'gemini-3.1-flash-image-001',
};

/**
 * 检查 token 是否还有其他可用的核心模型组
 * @param {string} tokenId - Token ID
 * @param {string} [groupingMode='default'] - 分组模式
 * @returns {boolean} 是否有其他可用模型组
 */
export function hasOtherAvailableModelGroups(tokenId, groupingMode = 'default') {
  for (const [, modelId] of Object.entries(CORE_GROUP_REPRESENTATIVES)) {
    if (tokenCooldownManager.isAvailable(tokenId, modelId, groupingMode)) {
      return true;
    }
  }
  return false;
}

/**
 * 获取 token 当前可用的核心模型组列表
 * @param {string} tokenId - Token ID
 * @param {string} [groupingMode='default'] - 分组模式
 * @returns {string[]} 可用的模型组键名数组
 */
export function getAvailableModelGroups(tokenId, groupingMode = 'default') {
  return Object.entries(CORE_GROUP_REPRESENTATIVES)
    .filter(([, modelId]) => tokenCooldownManager.isAvailable(tokenId, modelId, groupingMode))
    .map(([groupKey]) => groupKey);
}
