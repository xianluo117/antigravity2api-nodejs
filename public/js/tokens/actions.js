async function fetchProjectId(event, tokenId) {
  event.stopPropagation();

  const btn = event.target;
  btn.disabled = true;
  btn.textContent = "⏳";

  try {
    const response = await authFetch(
      `/admin/tokens/${encodeURIComponent(tokenId)}/fetch-project-id`,
      {
        method: "POST",
      },
    );

    const data = await response.json();
    if (data.success) {
      showToast(`Project ID 获取成功: ${data.projectId}`, "success");
      loadTokens();
    } else {
      showToast(`获取失败: ${data.message || "未知错误"}`, "error");
      btn.disabled = false;
      btn.textContent = "🔍";
    }
  } catch (error) {
    if (error.message !== "Unauthorized") {
      showToast(`获取失败: ${error.message}`, "error");
    }
    btn.disabled = false;
    btn.textContent = "🔍";
  }
}

async function batchFetchProjectIds() {
  if (!cachedTokens || cachedTokens.length === 0) {
    showToast("没有可用的 Token", "warning");
    return;
  }

  const enabledTokens = cachedTokens.filter((token) => token.enable);
  if (enabledTokens.length === 0) {
    showToast("没有启用的 Token", "warning");
    return;
  }

  showLoading(`正在批量获取 Project ID (0/${enabledTokens.length})...`);

  let successCount = 0;
  let failCount = 0;

  for (let i = 0; i < enabledTokens.length; i++) {
    const token = enabledTokens[i];
    updateLoadingText(`正在批量获取 Project ID (${i + 1}/${enabledTokens.length})...`);

    try {
      const response = await authFetch(
        `/admin/tokens/${encodeURIComponent(token.id)}/fetch-project-id`,
        {
          method: "POST",
        },
      );
      const data = await response.json();
      if (data.success) {
        successCount++;
      } else {
        failCount++;
      }
    } catch {
      failCount++;
    }

    if (i < enabledTokens.length - 1) {
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  }

  hideLoading();
  showToast(
    `批量获取完成: 成功 ${successCount} 个，失败 ${failCount} 个`,
    successCount > 0 ? "success" : "error",
  );
  loadTokens();
}

async function exportTokens() {
  const password = await showPasswordPrompt("请输入管理员密码以导出 Token");
  if (!password) return;

  showLoading("正在导出...");
  try {
    const response = await authFetch("/admin/tokens/export", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ password }),
    });

    const data = await response.json();
    hideLoading();

    if (data.success) {
      const blob = new Blob([JSON.stringify(data.data, null, 2)], {
        type: "application/json",
      });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `tokens-export-${new Date().toISOString().slice(0, 10)}.json`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
      showToast("导出成功", "success");
    } else if (response.status === 403) {
      showToast("密码错误，请重新输入", "error");
    } else {
      showToast(data.message || "导出失败", "error");
    }
  } catch (error) {
    hideLoading();
    showToast("导出失败: " + error.message, "error");
  }
}

async function importTokens() {
  showImportUploadModal();
}

function filterTokens(filter) {
  currentFilter = filter;
  localStorage.setItem("tokenFilter", filter);

  updateFilterButtonState(filter);
  renderTokens(cachedTokens);
}

async function loadTokens() {
  try {
    const response = await authFetch("/admin/tokens");
    const data = await response.json();
    if (data.success) {
      renderTokens(data.data);
    } else {
      showToast("加载失败: " + (data.message || "未知错误"), "error");
    }
  } catch (error) {
    showToast("加载Token失败: " + error.message, "error");
  }
}
