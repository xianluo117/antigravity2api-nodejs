import crypto from "crypto";
import config from "../config/config.js";
import {
  GEMINICLI_OAUTH_CONFIG,
  GEMINICLI_OAUTH_SCOPES,
  OAUTH_CONFIG,
  OAUTH_SCOPES,
} from "../constants/oauth.js";
import log from "../utils/logger.js";
import requesterManager from "../utils/requesterManager.js";
import tokenManager from "./token_manager.js";

class OAuthManager {
  constructor() {
    this.state = crypto.randomUUID();
  }

  /**
   * 生成授权URL
   * @param {number} port - 回调端口
   * @param {string} mode - 模式：'antigravity' 或 'geminicli'
   */
  generateAuthUrl(port, mode = "antigravity") {
    const oauthConfig =
      mode === "geminicli" ? GEMINICLI_OAUTH_CONFIG : OAUTH_CONFIG;
    const scopes = mode === "geminicli" ? GEMINICLI_OAUTH_SCOPES : OAUTH_SCOPES;

    // 使用与前端一致的 URL 构建方式（encodeURIComponent 编码，空格为 %20）
    const redirectUri = `http://localhost:${port}/oauth-callback`;
    const scopeStr = scopes.join(" ");
    const state =
      mode === "geminicli"
        ? `geminicli_${Date.now()}_${port}`
        : `${Date.now()}_${port}`;

    return (
      `${oauthConfig.AUTH_URL}?` +
      `access_type=offline&client_id=${oauthConfig.CLIENT_ID}&prompt=consent&` +
      `redirect_uri=${encodeURIComponent(redirectUri)}&response_type=code&` +
      `scope=${encodeURIComponent(scopeStr)}&state=${state}`
    );
  }

  /**
   * 交换授权码获取Token
   * @param {string} code - 授权码
   * @param {number} port - 回调端口
   * @param {string} mode - 模式：'antigravity' 或 'geminicli'
   */
  async exchangeCodeForToken(code, port, mode = "antigravity") {
    const oauthConfig =
      mode === "geminicli" ? GEMINICLI_OAUTH_CONFIG : OAUTH_CONFIG;

    const postData = new URLSearchParams({
      code,
      client_id: oauthConfig.CLIENT_ID,
      client_secret: oauthConfig.CLIENT_SECRET,
      redirect_uri: `http://localhost:${port}/oauth-callback`,
      grant_type: "authorization_code",
    });

    const headers = {
      Host: "oauth2.googleapis.com",
      "User-Agent": "Go-http-client/1.1",
      "Content-Type": "application/x-www-form-urlencoded",
      "Accept-Encoding": "gzip",
    };

    const { data } = await requesterManager.fetch(oauthConfig.TOKEN_URL, {
      method: "POST",
      headers,
      body: postData.toString(),
      timeout: config.timeout,
    });
    return data;
  }

  /**
   * 获取用户邮箱
   */
  async fetchUserEmail(accessToken) {
    const headers = {
      Host: "www.googleapis.com",
      "User-Agent": "Go-http-client/1.1",
      Authorization: `Bearer ${accessToken}`,
      "Accept-Encoding": "gzip",
    };

    try {
      const { data } = await requesterManager.fetch(
        "https://www.googleapis.com/oauth2/v2/userinfo",
        {
          method: "GET",
          headers,
          timeout: config.timeout,
        },
      );
      return data?.email;
    } catch (err) {
      log.warn("获取用户邮箱失败:", err.message);
      return null;
    }
  }

  /**
   * 资格校验：尝试获取projectId
   */
  async validateAndGetProjectId(accessToken) {
    try {
      log.info("正在验证账号资格...");
      const { projectId, sub } =
        (await tokenManager.fetchProjectId({ access_token: accessToken })) ||
        {};

      if (projectId === undefined || projectId === null) {
        log.warn("该账号无法获取 projectId，可能无资格或需要稍后重试");
        return { projectId: null, hasQuota: false, sub };
      }

      log.info("账号验证通过，projectId: " + projectId);
      return { projectId, hasQuota: true, sub };
    } catch (err) {
      log.error("验证账号资格失败: " + err.message);
      sub = "free-tier";
      return { projectId: null, hasQuota: false, sub };
    }
  }

  /**
   * 完整的OAuth认证流程：交换Token -> 获取邮箱 -> 资格校验
   * @param {string} code - 授权码
   * @param {number} port - 回调端口
   * @param {string} mode - 模式：'antigravity' 或 'geminicli'
   */
  async authenticate(code, port, mode = "antigravity") {
    // 1. 交换授权码获取Token
    const tokenData = await this.exchangeCodeForToken(code, port, mode);

    if (!tokenData.access_token) {
      throw new Error("Token交换失败：未获取到access_token");
    }

    const account = {
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token,
      expires_in: tokenData.expires_in,
      timestamp: Date.now(),
    };

    // 2. 获取用户邮箱
    const email = await this.fetchUserEmail(account.access_token);
    if (email) {
      account.email = email;
      log.info(`[${mode}] 获取到用户邮箱: ${email}`);
    }

    // 3. 资格校验（仅 antigravity 模式需要 projectId）
    if (mode === "antigravity") {
      const { projectId, hasQuota, sub, credits } =
        await this.validateAndGetProjectId(account.access_token);
      account.projectId = projectId;
      account.hasQuota = hasQuota;
      account.sub = sub;
      if (credits !== null && credits !== undefined) {
        account.credits = credits;
      }
    }

    account.enable = true;

    return account;
  }

  /**
   * Gemini CLI 专用认证流程（简化版，不需要 projectId）
   * @param {string} code - 授权码
   * @param {number} port - 回调端口
   */
  async authenticateGeminiCli(code, port) {
    return this.authenticate(code, port, "geminicli");
  }
}

export default new OAuthManager();
