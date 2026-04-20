function downloadTokenExportPayload(payload, filenamePrefix) {
  const blob = new Blob([JSON.stringify(payload, null, 2)], {
    type: "application/json",
  });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `${filenamePrefix}-${new Date().toISOString().slice(0, 10)}.json`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

async function executeTokenBatchAction(action, options = {}) {
  const tokenIds = [...selectedTokenIds];
  if (tokenIds.length === 0) {
    showToast("请先选择要操作的 Token", "warning");
    return null;
  }

  if (options.confirmMessage) {
    const confirmed = await showConfirm(
      options.confirmMessage,
      options.confirmTitle || "批量操作确认",
    );
    if (!confirmed) return null;
  }

  let password = null;
  if (options.requirePassword) {
    password = await showPasswordPrompt(options.passwordPrompt);
    if (!password) return null;
  }

  showLoading(options.loadingText || "正在批量处理...");
  try {
    const response = await authFetch("/admin/tokens/batch", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ action, tokenIds, password }),
    });

    const data = await response.json();
    hideLoading();

    if (!data.success) {
      showToast(data.message || "批量操作失败", "error");
      return null;
    }

    const payload = data.data || {};
    const hasFailure = Number(payload.failCount) > 0;
    const toastType = hasFailure ? "warning" : "success";

    if (action === "export") {
      downloadTokenExportPayload(
        payload.exportData || { tokens: [] },
        "tokens-selected-export",
      );
      showToast(data.message || "导出成功", toastType);
    } else {
      showToast(data.message || "批量操作成功", toastType);
    }

    if (action === "refresh_quota" && typeof quotaCache !== "undefined") {
      tokenIds.forEach((tokenId) =>
        quotaCache.clear(getQuotaCacheKey(tokenId, "antigravity")),
      );
    }

    selectedTokenIds.clear();
    skipAnimation = true;
    await loadTokens();
    return payload;
  } catch (error) {
    hideLoading();
    showToast(`批量操作失败: ${error.message}`, "error");
    return null;
  }
}

function toggleTokenSelection(tokenId, checked, event) {
  event?.stopPropagation?.();
  if (checked) {
    selectedTokenIds.add(tokenId);
  } else {
    selectedTokenIds.delete(tokenId);
  }
  renderTokens(cachedTokens);
}

function toggleSelectAllTokens() {
  const filteredTokenIds = getFilteredTokens().map((token) => token.id);
  const allSelected =
    filteredTokenIds.length > 0 &&
    filteredTokenIds.every((tokenId) => selectedTokenIds.has(tokenId));

  if (allSelected) {
    filteredTokenIds.forEach((tokenId) => selectedTokenIds.delete(tokenId));
  } else {
    filteredTokenIds.forEach((tokenId) => selectedTokenIds.add(tokenId));
  }

  renderTokens(cachedTokens);
}

function clearTokenSelection() {
  if (selectedTokenIds.size === 0) return;
  selectedTokenIds.clear();
  renderTokens(cachedTokens);
}

async function batchEnableSelectedTokens() {
  await executeTokenBatchAction("enable", {
    confirmTitle: "批量启用确认",
    confirmMessage: `确定要批量启用已选中的 ${selectedTokenIds.size} 个 Token 吗？\n系统会逐个验证凭证可用性。`,
    loadingText: "正在批量验证并启用 Token...",
  });
}

async function batchDisableSelectedTokens() {
  await executeTokenBatchAction("disable", {
    confirmTitle: "批量禁用确认",
    confirmMessage: `确定要批量禁用已选中的 ${selectedTokenIds.size} 个 Token 吗？`,
    loadingText: "正在批量禁用 Token...",
  });
}

async function batchFetchSelectedProjectIds() {
  await executeTokenBatchAction("fetch_project_id", {
    loadingText: "正在批量获取 Project ID...",
  });
}

async function batchRefreshSelectedTokenQuotas() {
  await executeTokenBatchAction("refresh_quota", {
    loadingText: "正在批量刷新额度...",
  });
}

async function batchReloadSelectedTokens() {
  await executeTokenBatchAction("refresh_token", {
    loadingText: "正在批量重载凭证...",
  });
}

async function batchDeleteSelectedTokens() {
  await executeTokenBatchAction("delete", {
    confirmTitle: "批量删除确认",
    confirmMessage: `删除后无法恢复，确定要批量删除已选中的 ${selectedTokenIds.size} 个 Token 吗？`,
    loadingText: "正在批量删除 Token...",
  });
}

async function batchExportSelectedTokens() {
  await executeTokenBatchAction("export", {
    requirePassword: true,
    passwordPrompt: "请输入管理员密码以导出选中的 Token",
    loadingText: "正在导出选中的 Token...",
  });
}
