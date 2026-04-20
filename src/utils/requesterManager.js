import axios from "axios";
import path from "path";
import { fileURLToPath } from "url";
import config from "../config/config.js";
import fingerprintRequester from "../requester.js";
import { buildAxiosRequestConfig } from "./httpClient.js";
import logger from "./logger.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

class RequesterManager {
  constructor() {
    this._tlsRequester = null;
    this._useAxios = false;
    this._initialized = false;
    this._configSignature = null;
  }

  _getConfigSignature() {
    return JSON.stringify({
      timeout: config.timeout,
      useNativeAxios: config.useNativeAxios === true,
      isPkg: typeof process.pkg !== "undefined",
      execPath: process.execPath,
    });
  }

  _resolveConfigPath() {
    const isPkg = typeof process.pkg !== "undefined";
    return isPkg
      ? path.join(path.dirname(process.execPath), "bin", "tls_config.json")
      : path.join(__dirname, "..", "bin", "tls_config.json");
  }

  _resetState({ closeTls = true } = {}) {
    if (closeTls && this._tlsRequester) {
      try {
        this._tlsRequester.close();
      } catch {}
    }
    this._tlsRequester = null;
    this._useAxios = false;
    this._initialized = false;
    this._configSignature = null;
  }

  _ensureInit() {
    const nextSignature = this._getConfigSignature();
    if (this._initialized && this._configSignature === nextSignature) {
      return;
    }

    this._resetState();
    this._configSignature = nextSignature;

    if (config.useNativeAxios === true) {
      this._useAxios = true;
      this._initialized = true;
      logger.info("[RequesterManager] 使用原生 axios 请求");
      return;
    }

    try {
      this._tlsRequester = fingerprintRequester.create({
        configPath: this._resolveConfigPath(),
        timeout: config.timeout ? Math.ceil(config.timeout / 1000) : 30,
      });
      this._useAxios = false;
      logger.info("[RequesterManager] 使用 FingerprintRequester 请求");
    } catch (error) {
      this._tlsRequester = null;
      this._useAxios = true;
      logger.warn(
        "[RequesterManager] FingerprintRequester 初始化失败，自动降级使用 axios:",
        error.message,
      );
    }

    this._initialized = true;
  }

  _shouldUseAxiosForBody(body) {
    return Buffer.isBuffer(body) || body instanceof Uint8Array;
  }

  _buildTlsConfig(method, headers, body, timeout, proxy) {
    const selectedProxy = proxy && proxy.enabled !== false ? proxy : null;
    const fallbackProxy =
      config.proxy?.allRequests && config.proxy?.enabled !== false
        ? config.proxy
        : null;
    const effectiveProxy = selectedProxy || fallbackProxy;
    const reqConfig = {
      method,
      headers,
      timeout_ms: timeout ?? config.timeout,
    };

    if (effectiveProxy) {
      reqConfig.proxy = effectiveProxy;
    }

    if (body !== null) {
      reqConfig.body = typeof body === "string" ? body : JSON.stringify(body);
    }

    return reqConfig;
  }

  async fetch(
    url,
    {
      method = "POST",
      headers = {},
      body = null,
      okStatus = [200],
      responseType,
      timeout = null,
      proxy = null,
    } = {},
  ) {
    this._ensureInit();

    if (this._useAxios || this._shouldUseAxiosForBody(body)) {
      return this._axiosFetch(url, {
        method,
        headers,
        body,
        okStatus,
        responseType,
        timeout,
        proxy,
      });
    }

    return this._tlsFetch(url, {
      method,
      headers,
      body,
      okStatus,
      responseType,
      timeout,
      proxy,
    });
  }

  async fetchStream(
    url,
    {
      method = "POST",
      headers = {},
      body = null,
      timeout = null,
      proxy = null,
    } = {},
  ) {
    this._ensureInit();

    if (this._useAxios || this._shouldUseAxiosForBody(body)) {
      return this._axiosFetchStream(url, {
        method,
        headers,
        body,
        timeout,
        proxy,
      });
    }

    return this._tlsFetchStream(url, {
      method,
      headers,
      body,
      timeout,
      proxy,
    });
  }

  async _tlsFetch(
    url,
    { method, headers, body, okStatus, responseType, timeout, proxy },
  ) {
    const reqConfig = this._buildTlsConfig(
      method,
      headers,
      body,
      timeout,
      proxy,
    );
    const response = await this._tlsRequester.antigravity_fetch(url, reqConfig);

    if (!okStatus.includes(response.status)) {
      const errorBody = await response.text();
      throw { status: response.status, message: errorBody };
    }

    const rawText = await response.text();
    let data = rawText;

    if (responseType !== "text" && rawText) {
      try {
        data = JSON.parse(rawText);
      } catch {
        data = rawText;
      }
    }

    return {
      status: response.status,
      data,
      headers: Object.fromEntries(response.headers || []),
    };
  }

  _tlsFetchStream(url, { method, headers, body, timeout, proxy }) {
    const reqConfig = this._buildTlsConfig(
      method,
      headers,
      body,
      timeout,
      proxy,
    );
    return this._tlsRequester.antigravity_fetchStream(url, reqConfig);
  }

  async _axiosFetch(
    url,
    { method, headers, body, okStatus, responseType, timeout, proxy },
  ) {
    const axiosConfig = buildAxiosRequestConfig({
      method,
      url,
      headers,
      data: body,
      timeout: timeout ?? config.timeout,
      responseType,
      proxy,
      useChunked: body !== null,
      validateStatus: () => true,
    });

    const response = await axios(axiosConfig);

    if (!okStatus.includes(response.status)) {
      const errorBody =
        typeof response.data === "string"
          ? response.data
          : JSON.stringify(response.data);
      throw { status: response.status, message: errorBody };
    }

    return {
      status: response.status,
      data: response.data,
      headers: response.headers,
    };
  }

  _axiosFetchStream(url, { method, headers, body, timeout, proxy }) {
    const streamResponse = new AxiosStreamResponse();

    const axiosConfig = buildAxiosRequestConfig({
      method,
      url,
      headers,
      data: body,
      timeout: timeout ?? config.timeout,
      proxy,
      responseType: "stream",
      useChunked: body !== null,
      validateStatus: () => true,
    });

    axios(axiosConfig)
      .then((response) => {
        streamResponse.status = response.status;
        streamResponse.headers = new Map(
          Object.entries(response.headers || {}),
        );

        if (streamResponse._onStart) {
          streamResponse._onStart({
            status: response.status,
            headers: streamResponse.headers,
          });
        }

        response.data.on("data", (chunk) => {
          if (streamResponse._onData) {
            streamResponse._onData(chunk.toString("utf8"));
          }
        });

        response.data.on("end", () => {
          if (streamResponse._onEnd) {
            streamResponse._onEnd();
          }
        });

        response.data.on("error", (error) => {
          if (streamResponse._onError) {
            streamResponse._onError(error);
          }
        });
      })
      .catch((error) => {
        if (streamResponse._onError) {
          streamResponse._onError(error);
        }
      });

    return streamResponse;
  }

  reloadConfig() {
    this._resetState();
  }

  close() {
    this._resetState();
  }
}

class AxiosStreamResponse {
  constructor() {
    this.status = null;
    this.headers = new Map();
    this._onStart = null;
    this._onData = null;
    this._onEnd = null;
    this._onError = null;
  }

  onStart(callback) {
    this._onStart = callback;
    return this;
  }

  onData(callback) {
    this._onData = callback;
    return this;
  }

  onEnd(callback) {
    this._onEnd = callback;
    return this;
  }

  onError(callback) {
    this._onError = callback;
    return this;
  }
}

export default new RequesterManager();
