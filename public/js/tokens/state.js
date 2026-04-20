// Token 管理共享状态与基础工具

var cachedTokens = [];
var currentFilter = localStorage.getItem("tokenFilter") || "all";
var skipAnimation = false;
var selectedTokenIds = new Set();
var actionBarCollapsed = localStorage.getItem("actionBarCollapsed") === "true";
var eventListenerRegistry = new WeakMap();
var currentImportTab = "file";
var importModalHandlers = null;
var pendingImportData = null;
var refreshingTokens = new Set();

function getFilteredTokens(tokens = cachedTokens) {
  if (currentFilter === "enabled") {
    return tokens.filter((token) => token.enable);
  }
  if (currentFilter === "disabled") {
    return tokens.filter((token) => !token.enable);
  }
  return tokens;
}

function syncSelectedTokenIds() {
  const validIds = new Set((cachedTokens || []).map((token) => token.id));
  selectedTokenIds = new Set(
    [...selectedTokenIds].filter((tokenId) => validIds.has(tokenId)),
  );
}

function updateTokenBatchActionState() {
  const countEl = document.getElementById("selectedTokenCount");
  if (countEl) {
    countEl.textContent = selectedTokenIds.size;
  }

  const filteredTokens = getFilteredTokens();
  const hasFilteredTokens = filteredTokens.length > 0;
  const allVisibleSelected =
    hasFilteredTokens &&
    filteredTokens.every((token) => selectedTokenIds.has(token.id));

  const selectAllBtn = document.getElementById("tokenSelectAllBtn");
  if (selectAllBtn) {
    selectAllBtn.textContent = allVisibleSelected
      ? "☑️ 取消全选"
      : "☑️ 全选当前";
    selectAllBtn.disabled = !hasFilteredTokens;
  }

  const clearBtn = document.getElementById("tokenClearSelectionBtn");
  if (clearBtn) {
    clearBtn.disabled = selectedTokenIds.size === 0;
  }

  document.querySelectorAll("[data-token-bulk-action]").forEach((button) => {
    button.disabled = selectedTokenIds.size === 0;
  });

  const panel = document.getElementById("tokenBulkPanel");
  if (panel) {
    panel.classList.toggle("is-empty", selectedTokenIds.size === 0);
  }
}

function registerEventListener(element, event, handler, options) {
  if (!element) return;
  element.addEventListener(event, handler, options);

  if (!eventListenerRegistry.has(element)) {
    eventListenerRegistry.set(element, []);
  }
  eventListenerRegistry.get(element).push({ event, handler, options });
}

function cleanupEventListeners(element) {
  if (!element || !eventListenerRegistry.has(element)) return;

  const listeners = eventListenerRegistry.get(element);
  for (const { event, handler, options } of listeners) {
    element.removeEventListener(event, handler, options);
  }
  eventListenerRegistry.delete(element);
}

function isRandomProjectId(projectId) {
  if (!projectId) return true;
  const randomPattern = /^[a-z]+-[a-z]+-[a-z0-9]{5}$/;
  return randomPattern.test(projectId);
}

function updateLoadingText(text) {
  const loadingText = document.querySelector(".loading-overlay .loading-text");
  if (loadingText) {
    loadingText.textContent = text;
  }
}

function cleanupRefreshingTokens() {
  if (refreshingTokens.size > 100) {
    refreshingTokens.clear();
  }
}

function toggleActionBar() {
  const actionBar = document.getElementById("actionBar");
  const toggleBtn = document.getElementById("actionToggleBtn");

  if (!actionBar || !toggleBtn) return;

  actionBarCollapsed = !actionBarCollapsed;
  localStorage.setItem("actionBarCollapsed", actionBarCollapsed);

  if (actionBarCollapsed) {
    actionBar.classList.add("collapsed");
    toggleBtn.classList.add("collapsed");
    toggleBtn.title = "展开操作按钮";
  } else {
    actionBar.classList.remove("collapsed");
    toggleBtn.classList.remove("collapsed");
    toggleBtn.title = "收起操作按钮";
  }
}

window.toggleActionBar = toggleActionBar;

function initActionBarState() {
  const actionBar = document.getElementById("actionBar");
  const toggleBtn = document.getElementById("actionToggleBtn");

  if (!actionBar || !toggleBtn) return;

  if (actionBarCollapsed) {
    actionBar.classList.add("collapsed");
    toggleBtn.classList.add("collapsed");
    toggleBtn.title = "展开操作按钮";
  }
}

function initFilterState() {
  const savedFilter = localStorage.getItem("tokenFilter") || "all";
  currentFilter = savedFilter;
  updateFilterButtonState(savedFilter);
}

function updateFilterButtonState(filter) {
  document.querySelectorAll(".stat-item").forEach((item) => {
    item.classList.remove("active");
  });
  const filterMap = {
    all: "totalTokens",
    enabled: "enabledTokens",
    disabled: "disabledTokens",
  };
  const activeElement = document.getElementById(filterMap[filter]);
  if (activeElement) {
    activeElement.closest(".stat-item").classList.add("active");
  }
}
