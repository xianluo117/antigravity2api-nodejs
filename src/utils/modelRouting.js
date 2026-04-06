export const ANTIGRAVITY_MODEL_PREFIX = "an-";
export const GEMINICLI_MODEL_PREFIX = "cli-";

const GEMINICLI_BASE_MODELS = [
  "gemini-2.5-pro",
  "gemini-2.5-flash",
  "gemini-3-pro-preview",
  "gemini-3-flash-preview",
  "gemini-3.1-pro-preview",
];

const GEMINICLI_FEATURE_PREFIXES = ["", "假流式/", "流式抗截断/"];
const GEMINICLI_THINKING_SUFFIXES = ["", "-maxthinking", "-nothinking"];
const GEMINICLI_SEARCH_SUFFIXES = ["", "-search"];

export function isAntigravityModel(modelName) {
  return (
    typeof modelName === "string" &&
    modelName.startsWith(ANTIGRAVITY_MODEL_PREFIX)
  );
}

export function isGeminiCliModel(modelName) {
  return (
    typeof modelName === "string" &&
    modelName.startsWith(GEMINICLI_MODEL_PREFIX)
  );
}

export function stripPublicModelPrefix(modelName) {
  if (typeof modelName !== "string") return modelName;

  if (isAntigravityModel(modelName)) {
    return modelName.slice(ANTIGRAVITY_MODEL_PREFIX.length);
  }

  if (isGeminiCliModel(modelName)) {
    return modelName.slice(GEMINICLI_MODEL_PREFIX.length);
  }

  return modelName;
}

export function toAntigravityPublicModelId(actualModelName) {
  return `${ANTIGRAVITY_MODEL_PREFIX}${stripPublicModelPrefix(actualModelName)}`;
}

export function toGeminiCliPublicModelId(actualModelName) {
  return `${GEMINICLI_MODEL_PREFIX}${stripPublicModelPrefix(actualModelName)}`;
}

export function getGeminiCliActualModels() {
  const models = [];
  const seen = new Set();

  for (const baseModel of GEMINICLI_BASE_MODELS) {
    for (const featurePrefix of GEMINICLI_FEATURE_PREFIXES) {
      for (const thinkingSuffix of GEMINICLI_THINKING_SUFFIXES) {
        for (const searchSuffix of GEMINICLI_SEARCH_SUFFIXES) {
          const modelName = `${featurePrefix}${baseModel}${thinkingSuffix}${searchSuffix}`;
          if (!seen.has(modelName)) {
            seen.add(modelName);
            models.push(modelName);
          }
        }
      }
    }
  }

  return models;
}

export function getPrefixedGeminiCliModels() {
  return getGeminiCliActualModels().map((modelName) =>
    toGeminiCliPublicModelId(modelName),
  );
}
