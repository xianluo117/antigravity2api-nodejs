/**
 * OpenAI 格式处理器
 * 处理 /v1/chat/completions 请求，支持流式和非流式响应
 */

import {
  generateAssistantResponse,
  generateAssistantResponseNoStream,
  getModelsWithQuotas,
} from "../../api/client.js";
import quotaManager from "../../auth/quota_manager.js";
import tokenManager from "../../auth/token_manager.js";
import config from "../../config/config.js";
import { buildOpenAIErrorPayload } from "../../utils/errors.js";
import logger from "../../utils/logger.js";
import {
  isGeminiCliModel,
  stripPublicModelPrefix,
} from "../../utils/modelRouting.js";
import { generateRequestBody, prepareImageRequest } from "../../utils/utils.js";
import {
  createOpenAIChatCompletionResponse,
  createOpenAIStreamChunk as createStreamChunk,
} from "../formatters/openai.js";
import {
  createHeartbeat,
  createResponseMeta,
  endStream,
  setStreamHeaders,
  with429Retry,
  writeStreamData,
} from "../stream.js";
import { validateIncomingChatRequest } from "../validators/chat.js";
import { getSafeRetries } from "./common/retry.js";
import { handleGeminiCliRequest } from "./geminicli.js";

/**
 * 处理 OpenAI 格式的聊天请求
 * @param {Request} req - Express请求对象
 * @param {Response} res - Express响应对象
 */
export const handleOpenAIRequest = async (req, res) => {
  const body = req.body || {};
  const { messages, model, stream = false, tools, ...params } = body;

  try {
    const validation = validateIncomingChatRequest("openai", body);
    if (!validation.ok) {
      return res.status(validation.status).json({ error: validation.message });
    }
    if (typeof model !== "string" || !model) {
      return res.status(400).json({ error: "model is required" });
    }

    if (isGeminiCliModel(model)) {
      return handleGeminiCliRequest(req, res, "openai");
    }

    const actualModel = stripPublicModelPrefix(model);
    const responseModel = model;

    const token = await tokenManager.getToken(actualModel);
    if (!token) {
      throw new Error("没有可用的token，请运行 npm run login 获取token");
    }

    // 获取 tokenId 用于冷却状态管理
    const tokenId = await tokenManager.getTokenId(token);

    // 创建刷新额度的回调函数
    const refreshQuota = async () => {
      if (!tokenId) return;
      const quotas = await getModelsWithQuotas(token);
      quotaManager.updateQuota(tokenId, quotas);
    };

    const applyCreditTypes = (body, shouldUseCredits) =>
      shouldUseCredits || config.alwaysUseCredits
        ? { ...body, enabledCreditTypes: ["GOOGLE_ONE_AI"] }
        : body;

    // 创建 with429Retry 选项
    const createRetryOptions = (prefix) => ({
      loggerPrefix: prefix,
      onAttempt: () => tokenManager.recordRequest(token, actualModel),
      tokenId,
      modelId: actualModel,
      refreshQuota,
      markQuotaExhausted: async () => tokenManager.markQuotaExhausted(token),
    });

    const isImageModel = actualModel.includes("-image");
    const requestBody = generateRequestBody(
      messages,
      actualModel,
      params,
      tools,
      token,
    );

    if (isImageModel) {
      prepareImageRequest(requestBody);
    }
    //console.log(JSON.stringify(requestBody,null,2));
    const { id, created } = createResponseMeta();
    const safeRetries = getSafeRetries(config.retryTimes);

    if (stream) {
      setStreamHeaders(res);

      // 启动心跳，防止 Cloudflare 超时断连
      const heartbeatTimer = createHeartbeat(res);

      try {
        if (isImageModel) {
          const { content, usage, reasoningSignature } = await with429Retry(
            (attempt, shouldUseCredits) =>
              generateAssistantResponseNoStream(
                applyCreditTypes(requestBody, shouldUseCredits),
                token,
              ),
            safeRetries,
            createRetryOptions("chat.stream.image "),
          );
          const delta = { content };
          if (reasoningSignature && config.passSignatureToClient) {
            delta.thoughtSignature = reasoningSignature;
          }
          writeStreamData(
            res,
            createStreamChunk(id, created, responseModel, delta),
          );
          writeStreamData(res, {
            ...createStreamChunk(id, created, responseModel, {}, "stop"),
            usage,
          });
        } else {
          let hasToolCall = false;
          let usageData = null;

          await with429Retry(
            (attempt, shouldUseCredits) =>
              generateAssistantResponse(
                applyCreditTypes(requestBody, shouldUseCredits),
                token,
                (data) => {
                  if (data.type === "usage") {
                    usageData = data.usage;
                  } else if (data.type === "reasoning") {
                    const delta = { reasoning_content: data.reasoning_content };
                    if (data.thoughtSignature && config.passSignatureToClient) {
                      delta.thoughtSignature = data.thoughtSignature;
                    }
                    writeStreamData(
                      res,
                      createStreamChunk(id, created, responseModel, delta),
                    );
                  } else if (data.type === "tool_calls") {
                    hasToolCall = true;
                    // 根据配置决定是否透传工具调用中的签名
                    const toolCallsWithIndex = data.tool_calls.map(
                      (toolCall, index) => {
                        if (config.passSignatureToClient) {
                          return { index, ...toolCall };
                        } else {
                          const { thoughtSignature, ...rest } = toolCall;
                          return { index, ...rest };
                        }
                      },
                    );
                    const delta = { tool_calls: toolCallsWithIndex };
                    writeStreamData(
                      res,
                      createStreamChunk(id, created, responseModel, delta),
                    );
                  } else {
                    const delta = { content: data.content };
                    writeStreamData(
                      res,
                      createStreamChunk(id, created, responseModel, delta),
                    );
                  }
                },
              ),
            safeRetries,
            createRetryOptions("chat.stream "),
          );

          writeStreamData(res, {
            ...createStreamChunk(
              id,
              created,
              responseModel,
              {},
              hasToolCall ? "tool_calls" : "stop",
            ),
            usage: usageData,
          });
        }

        clearInterval(heartbeatTimer);
        endStream(res);
      } catch (error) {
        clearInterval(heartbeatTimer);
        if (!res.writableEnded) {
          const statusCode = error.statusCode || error.status || 500;
          writeStreamData(res, buildOpenAIErrorPayload(error, statusCode));
          endStream(res);
        }
        logger.error("生成响应失败:", error.message);
        return;
      }
    } else if (config.fakeNonStream && !isImageModel) {
      // 假非流模式：使用流式API获取数据，组装成非流式响应
      req.setTimeout(0);
      res.setTimeout(0);

      let content = "";
      let reasoningContent = "";
      let reasoningSignature = null;
      const toolCalls = [];
      let usageData = null;

      try {
        await with429Retry(
          (attempt, shouldUseCredits) =>
            generateAssistantResponse(
              applyCreditTypes(requestBody, shouldUseCredits),
              token,
              (data) => {
                if (data.type === "usage") {
                  usageData = data.usage;
                } else if (data.type === "reasoning") {
                  reasoningContent += data.reasoning_content || "";
                  if (data.thoughtSignature) {
                    reasoningSignature = data.thoughtSignature;
                  }
                } else if (data.type === "tool_calls") {
                  toolCalls.push(...data.tool_calls);
                } else if (data.type === "text") {
                  content += data.content || "";
                }
              },
            ),
          safeRetries,
          createRetryOptions("chat.fake_no_stream "),
        );

        // 构建非流式响应
        const message = { role: "assistant" };
        if (reasoningContent) message.reasoning_content = reasoningContent;
        if (reasoningSignature && config.passSignatureToClient)
          message.thoughtSignature = reasoningSignature;
        message.content = content;

        if (toolCalls.length > 0) {
          if (config.passSignatureToClient) {
            message.tool_calls = toolCalls;
          } else {
            message.tool_calls = toolCalls.map(
              ({ thoughtSignature, ...rest }) => rest,
            );
          }
        }

        res.json(
          createOpenAIChatCompletionResponse({
            id,
            created,
            model: responseModel,
            content,
            reasoningContent,
            reasoningSignature,
            toolCalls,
            usage: usageData,
            passSignatureToClient: config.passSignatureToClient,
            stripToolCallSignature: !config.passSignatureToClient,
          }),
        );
      } catch (error) {
        logger.error("假非流生成响应失败:", error.message);
        if (res.headersSent) return;
        const statusCode = error.statusCode || error.status || 500;
        return res
          .status(statusCode)
          .json(buildOpenAIErrorPayload(error, statusCode));
      }
    } else {
      // 非流式请求：设置较长超时，避免大模型响应超时
      req.setTimeout(0); // 禁用请求超时
      res.setTimeout(0); // 禁用响应超时

      const {
        content,
        reasoningContent,
        reasoningSignature,
        toolCalls,
        usage,
      } = await with429Retry(
        (attempt, shouldUseCredits) =>
          generateAssistantResponseNoStream(
            applyCreditTypes(requestBody, shouldUseCredits),
            token,
          ),
        safeRetries,
        createRetryOptions("chat.no_stream "),
      );

      // DeepSeek 格式：reasoning_content 在 content 之前
      const message = { role: "assistant" };
      if (reasoningContent) message.reasoning_content = reasoningContent;
      if (reasoningSignature && config.passSignatureToClient)
        message.thoughtSignature = reasoningSignature;
      message.content = content;

      if (toolCalls.length > 0) {
        // 根据配置决定是否透传工具调用中的签名
        if (config.passSignatureToClient) {
          message.tool_calls = toolCalls;
        } else {
          message.tool_calls = toolCalls.map(
            ({ thoughtSignature, ...rest }) => rest,
          );
        }
      }

      // 使用预构建的响应对象，减少内存分配
      res.json(
        createOpenAIChatCompletionResponse({
          id,
          created,
          model: responseModel,
          content,
          reasoningContent,
          reasoningSignature,
          toolCalls,
          usage,
          passSignatureToClient: config.passSignatureToClient,
          stripToolCallSignature: !config.passSignatureToClient,
        }),
      );
    }
  } catch (error) {
    logger.error("生成响应失败:", error.message);
    if (res.headersSent) return;
    const statusCode = error.statusCode || error.status || 500;
    return res
      .status(statusCode)
      .json(buildOpenAIErrorPayload(error, statusCode));
  }
};
