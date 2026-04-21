import { TokenError } from "../../utils/errors.js";
import { generateTokenId } from "../../utils/idGenerator.js";
import { log } from "../../utils/logger.js";
import quotaManager from "../quota_manager.js";

export function saveToFile(tokenToUpdate = null) {
  this.store.mergeActiveTokens(this.tokens, tokenToUpdate).catch((error) => {
    log.error("保存账号配置文件失败:", error.message);
  });
}

export function disableToken(token, reason = "未知原因") {
  log.warn(`禁用token ...${token.access_token.slice(-8)}, 原因: ${reason}`);
  token.enable = false;
  token.disableReason = reason;
  token.disableTime = Date.now();
  token.lastError = reason;
  token.lastErrorTime = token.disableTime;
  token.lastErrorStage = "disable";
  this.saveToFile();
  this.tokenRequestCounts.delete(token.refresh_token);
  this.tokens = this.tokens.filter(
    (item) => item.refresh_token !== token.refresh_token,
  );
  this.currentIndex = this.currentIndex % Math.max(this.tokens.length, 1);
  this._rebuildAvailableQuotaTokens();
}

export function incrementRequestCount(tokenKey) {
  const current = this.tokenRequestCounts.get(tokenKey) || 0;
  const newCount = current + 1;
  this.tokenRequestCounts.set(tokenKey, newCount);
  return newCount;
}

export function resetRequestCount(tokenKey) {
  this.tokenRequestCounts.set(tokenKey, 0);
}

export function markQuotaExhausted(token) {
  token.hasQuota = false;
  this.saveToFile(token);
  log.warn(`...${token.access_token.slice(-8)}: 额度已耗尽，标记为无额度`);

  if (this.rotationStrategy === "quota_exhausted") {
    const tokenIndex = this.tokens.findIndex(
      (item) => item.refresh_token === token.refresh_token,
    );
    if (tokenIndex !== -1) {
      this._removeQuotaIndex(tokenIndex);
    }
    this.currentIndex = (this.currentIndex + 1) % Math.max(this.tokens.length, 1);
  }
}

export function restoreQuota(token) {
  token.hasQuota = true;
  this.saveToFile(token);
  log.info(`...${token.access_token.slice(-8)}: 额度已恢复`);
}

export async function recordRequest(token, modelId) {
  if (!token || !modelId) return;

  try {
    if (token.refresh_token) {
      this.incrementRequestCount(token.refresh_token);
    }
    const salt = await this.store.getSalt();
    const tokenId = generateTokenId(token.refresh_token, salt);
    quotaManager.recordRequest(tokenId, modelId);
  } catch (error) {
    log.warn("记录请求次数失败:", error.message);
  }
}

export function disableCurrentToken(
  token,
  reason = "API请求返回403，账号无使用权限",
) {
  const found = this.tokens.find((item) => item.access_token === token.access_token);
  if (found) {
    this.disableToken(found, reason);
  }
}

export async function reload() {
  this._initPromise = this._initialize();
  await this._initPromise;
  log.info("Token已热重载");
}

export async function addToken(tokenData) {
  try {
    const allTokens = await this.store.readAll();
    const salt = await this.store.getSalt();
    const tokenId = generateTokenId(tokenData.refresh_token, salt);

    const newToken = {
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token,
      expires_in: tokenData.expires_in || 3599,
      timestamp: tokenData.timestamp || Date.now(),
      enable: false,
    };

    if (tokenData.projectId) {
      newToken.projectId = tokenData.projectId;
    }
    if (tokenData.email) {
      newToken.email = tokenData.email;
    }
    if (tokenData.hasQuota !== undefined) {
      newToken.hasQuota = tokenData.hasQuota;
    }
    if (tokenData.sub) {
      newToken.sub = tokenData.sub;
    }
    if (tokenData.credits !== undefined) {
      newToken.credits = tokenData.credits;
    }

    allTokens.push(newToken);
    await this.store.writeAll(allTokens);
    await this.reload();

    const enableResult = await this.enableTokenById(tokenId, {
      stage: "oauth_submit",
    });
    if (!enableResult.success) {
      return {
        success: true,
        saved: true,
        validated: false,
        disabled: true,
        tokenId,
        message: enableResult.message || "Token已保存到禁用池",
      };
    }

    return {
      success: true,
      saved: true,
      validated: true,
      disabled: false,
      tokenId,
      message: "Token添加成功",
    };
  } catch (error) {
    log.error("添加Token失败:", error.message);
    return { success: false, message: error.message };
  }
}

export async function updateToken(refreshToken, updates) {
  try {
    const allTokens = await this.store.readAll();

    const index = allTokens.findIndex(
      (token) => token.refresh_token === refreshToken,
    );
    if (index === -1) {
      return { success: false, message: "Token不存在" };
    }

    if (updates.enable === true) {
      updates.disableReason = null;
      updates.disableTime = null;
    }
    allTokens[index] = { ...allTokens[index], ...updates };
    await this.store.writeAll(allTokens);

    await this.reload();
    return { success: true, message: "Token更新成功" };
  } catch (error) {
    log.error("更新Token失败:", error.message);
    return { success: false, message: error.message };
  }
}

export async function deleteToken(refreshToken) {
  try {
    const allTokens = await this.store.readAll();

    const filteredTokens = allTokens.filter(
      (token) => token.refresh_token !== refreshToken,
    );
    if (filteredTokens.length === allTokens.length) {
      return { success: false, message: "Token不存在" };
    }

    await this.store.writeAll(filteredTokens);

    await this.reload();
    return { success: true, message: "Token删除成功" };
  } catch (error) {
    log.error("删除Token失败:", error.message);
    return { success: false, message: error.message };
  }
}

export async function getTokenList() {
  try {
    const allTokens = await this.store.readAll();
    const salt = await this.store.getSalt();

    return allTokens.map((token) => ({
      id: generateTokenId(token.refresh_token, salt),
      expires_in: token.expires_in,
      timestamp: token.timestamp,
      enable: token.enable !== false,
      projectId: token.projectId || null,
      email: token.email || null,
      hasQuota: token.hasQuota !== false,
      sub: token.sub || "free-tier",
      credits:
        token.credits ?? ((token.sub || "free-tier") === "free-tier" ? 0 : null),
      disableReason: token.disableReason || null,
      disableTime: token.disableTime || null,
      lastError: token.lastError || null,
      lastErrorTime: token.lastErrorTime || null,
      lastErrorStage: token.lastErrorStage || null,
    }));
  } catch (error) {
    log.error("获取Token列表失败:", error.message);
    return [];
  }
}

export async function findTokenById(tokenId) {
  try {
    const allTokens = await this.store.readAll();
    const salt = await this.store.getSalt();

    return (
      allTokens.find(
        (token) => generateTokenId(token.refresh_token, salt) === tokenId,
      ) || null
    );
  } catch (error) {
    log.error("查找Token失败:", error.message);
    return null;
  }
}

export async function updateTokenById(tokenId, updates) {
  try {
    const allTokens = await this.store.readAll();
    const salt = await this.store.getSalt();

    const index = allTokens.findIndex(
      (token) => generateTokenId(token.refresh_token, salt) === tokenId,
    );

    if (index === -1) {
      return { success: false, message: "Token不存在" };
    }

    if (updates.enable === true) {
      updates.disableReason = null;
      updates.disableTime = null;
      updates.lastError = null;
      updates.lastErrorTime = null;
      updates.lastErrorStage = null;
    }

    allTokens[index] = { ...allTokens[index], ...updates };
    await this.store.writeAll(allTokens);

    await this.reload();
    return { success: true, message: "Token更新成功" };
  } catch (error) {
    log.error("更新Token失败:", error.message);
    return { success: false, message: error.message };
  }
}

export async function deleteTokenById(tokenId) {
  try {
    const allTokens = await this.store.readAll();
    const salt = await this.store.getSalt();

    const filteredTokens = allTokens.filter(
      (token) => generateTokenId(token.refresh_token, salt) !== tokenId,
    );

    if (filteredTokens.length === allTokens.length) {
      return { success: false, message: "Token不存在" };
    }

    await this.store.writeAll(filteredTokens);

    await this.reload();
    return { success: true, message: "Token删除成功" };
  } catch (error) {
    log.error("删除Token失败:", error.message);
    return { success: false, message: error.message };
  }
}

export async function refreshTokenById(tokenId) {
  const tokenData = await this.findTokenById(tokenId);
  if (!tokenData) {
    throw new TokenError("Token不存在", null, 404);
  }

  const refreshedToken = await this.refreshToken(tokenData);
  return {
    expires_in: refreshedToken.expires_in,
    timestamp: refreshedToken.timestamp,
    projectId: refreshedToken.projectId || null,
    sub: refreshedToken.sub || "free-tier",
    credits:
      refreshedToken.credits ??
      ((refreshedToken.sub || "free-tier") === "free-tier" ? 0 : null),
  };
}

export async function getSalt() {
  return this.store.getSalt();
}

export async function getTokenId(token) {
  if (!token?.refresh_token) return null;
  try {
    const salt = await this.store.getSalt();
    if (!salt) return null;
    return generateTokenId(token.refresh_token, salt);
  } catch (error) {
    log.error(`生成tokenId失败: ${error.message}`);
    return null;
  }
}

export function getRotationConfig() {
  return {
    strategy: this.rotationStrategy,
    requestCount: this.requestCountPerToken,
    currentIndex: this.currentIndex,
    totalTokens: this.tokens.length,
    tokenCounts: Object.fromEntries(this.tokenRequestCounts),
  };
}
