import config, { buildConfig, getConfigJson } from "../config/config.js";
import requesterManager from "./requesterManager.js";

/**
 * 重新加载配置到 config 对象
 */
export function reloadConfig() {
  const newConfig = buildConfig(getConfigJson());
  Object.assign(config, newConfig);
  requesterManager.reloadConfig();
}
