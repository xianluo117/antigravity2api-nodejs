async function manualRefreshToken(tokenId) {
  if (refreshingTokens.has(tokenId)) {
    showToast("该 Token 正在刷新中", "warning");
    return;
  }
  await autoRefreshToken(tokenId);
}

async function autoRefreshToken(tokenId) {
  if (refreshingTokens.has(tokenId)) return;

  refreshingTokens.add(tokenId);
  const cardId = tokenId.substring(0, 8);

  const card = document.getElementById(`card-${cardId}`);
  const refreshBtn = document.getElementById(`refresh-btn-${cardId}`);
  if (card) {
    card.classList.remove("refresh-failed");
    card.classList.add("refreshing");
  }
  if (refreshBtn) {
    refreshBtn.disabled = true;
    refreshBtn.classList.add("loading");
    refreshBtn.textContent = "🔄";
  }

  try {
    const response = await authFetch(
      `/admin/tokens/${encodeURIComponent(tokenId)}/refresh`,
      {
        method: "POST",
      },
    );

    const data = await response.json();
    if (data.success) {
      showToast("Token 已自动刷新", "success");
      refreshingTokens.delete(tokenId);
      if (card) card.classList.remove("refreshing");
      if (refreshBtn) {
        refreshBtn.disabled = false;
        refreshBtn.classList.remove("loading");
        refreshBtn.textContent = "🔄";
      }
      loadTokens();
    } else {
      showToast(`Token 刷新失败: ${data.message || "未知错误"}`, "error");
      refreshingTokens.delete(tokenId);
      if (card) {
        card.classList.remove("refreshing");
        card.classList.add("refresh-failed");
      }
      if (refreshBtn) {
        refreshBtn.disabled = false;
        refreshBtn.classList.remove("loading");
        refreshBtn.textContent = "🔄";
      }
    }
  } catch (error) {
    if (error.message !== "Unauthorized") {
      showToast(`Token 刷新失败: ${error.message}`, "error");
    }
    refreshingTokens.delete(tokenId);
    if (card) {
      card.classList.remove("refreshing");
      card.classList.add("refresh-failed");
    }
    if (refreshBtn) {
      refreshBtn.disabled = false;
      refreshBtn.classList.remove("loading");
      refreshBtn.textContent = "🔄";
    }
  }
}

function showManualModal() {
  showImportUploadModal();
  setTimeout(() => switchImportTab("manual"), 0);
}

function editField(event, tokenId, field, currentValue) {
  event.stopPropagation();
  const row = event.currentTarget;
  const valueSpan = row.querySelector(".info-value");

  if (row.querySelector("input")) return;

  const fieldLabels = { projectId: "Project ID", email: "邮箱" };

  const input = document.createElement("input");
  input.type = field === "email" ? "email" : "text";
  input.value = currentValue;
  input.className = "inline-edit-input";
  input.placeholder = `输入${fieldLabels[field]}`;

  valueSpan.style.display = "none";
  row.insertBefore(input, valueSpan.nextSibling);
  input.focus();
  input.select();

  const save = async () => {
    const newValue = input.value.trim();
    input.disabled = true;

    try {
      const response = await authFetch(
        `/admin/tokens/${encodeURIComponent(tokenId)}`,
        {
          method: "PUT",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ [field]: newValue }),
        },
      );

      const data = await response.json();
      if (data.success) {
        showToast("已保存", "success");
        loadTokens();
      } else {
        showToast(data.message || "保存失败", "error");
        cancel();
      }
    } catch {
      showToast("保存失败", "error");
      cancel();
    }
  };

  const cancel = () => {
    input.remove();
    valueSpan.style.display = "";
  };

  input.addEventListener("blur", () => {
    setTimeout(() => {
      if (document.activeElement !== input) {
        if (input.value.trim() !== currentValue) {
          save();
        } else {
          cancel();
        }
      }
    }, 100);
  });

  input.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      save();
    } else if (event.key === "Escape") {
      cancel();
    }
  });
}

function showTokenDetail(tokenId) {
  const token = cachedTokens.find((item) => item.id === tokenId);
  if (!token) {
    showToast("Token不存在", "error");
    return;
  }

  const safeTokenId = escapeJs(tokenId);
  const safeProjectId = escapeHtml(token.projectId || "");
  const safeEmail = escapeHtml(token.email || "");
  const subscription = String(token.sub || "free-tier");
  const subscriptionLabel =
    typeof formatTokenSubscriptionLabel === "function"
      ? formatTokenSubscriptionLabel(subscription)
      : subscription;
  const creditsText =
    typeof formatTokenCreditsValue === "function"
      ? formatTokenCreditsValue(token.credits)
      : String(token.credits ?? "未知");
  const updatedAtStr = escapeHtml(
    token.timestamp ? new Date(token.timestamp).toLocaleString("zh-CN") : "未知",
  );
  const disableReason = token.disableReason ? escapeHtml(token.disableReason) : "";
  const disableTimeStr = token.disableTime
    ? new Date(token.disableTime).toLocaleString("zh-CN")
    : "";
  const lastError = token.lastError ? escapeHtml(token.lastError) : "";
  const lastErrorTimeStr = token.lastErrorTime
    ? new Date(token.lastErrorTime).toLocaleString("zh-CN")
    : "";
  const lastErrorStageLabel =
    token.lastErrorStage === "startup_refresh"
      ? "启动检测"
      : token.lastErrorStage === "disable"
        ? "禁用"
        : token.lastErrorStage === "manual"
          ? "手动"
          : token.lastErrorStage === "request"
            ? "请求"
            : token.lastErrorStage === "enable_test"
              ? "启用验证"
              : token.lastErrorStage === "oauth_submit"
                ? "OAuth提交校验"
                : token.lastErrorStage || "";

  const modal = document.createElement("div");
  modal.className = "modal form-modal";
  modal.innerHTML = `
        <div class="modal-content">
            <div class="modal-title">📝 Token详情</div>
            <div class="form-group compact">
                <label>🔑 Token ID</label>
                <div class="token-display">${escapeHtml(tokenId)}</div>
            </div>
            <div class="form-group compact">
                <label>📦 Project ID</label>
                <input type="text" id="editProjectId" value="${safeProjectId}" placeholder="项目ID">
            </div>
            <div class="form-group compact">
                <label>📧 邮箱</label>
                <input type="email" id="editEmail" value="${safeEmail}" placeholder="账号邮箱">
            </div>
            <div class="form-group compact">
                <label>⭐ 订阅等级</label>
                <input type="text" value="${escapeHtml(subscriptionLabel)}" readonly style="background: var(--bg); cursor: not-allowed;">
            </div>
            <div class="form-group compact">
                <label>💳 积分余额</label>
                <input type="text" value="${escapeHtml(creditsText)}" readonly style="background: var(--bg); cursor: not-allowed;">
            </div>
            <div class="form-group compact">
                <label>🕒 最后更新时间</label>
                <input type="text" value="${updatedAtStr}" readonly style="background: var(--bg); cursor: not-allowed;">
            </div>
            ${
              lastError
                ? `
            <div class="form-group compact">
                <label>🧾 最近错误</label>
                <div class="token-error-detail" style="max-height: 8em; overflow-y: auto;">
                    ${lastError}
                    ${lastErrorTimeStr || lastErrorStageLabel ? `<br><span class="token-error-meta">${lastErrorTimeStr ? "记录时间: " + lastErrorTimeStr : ""}${lastErrorTimeStr && lastErrorStageLabel ? " · " : ""}${lastErrorStageLabel ? "来源: " + lastErrorStageLabel : ""}</span>` : ""}
                </div>
                ${render403ActionUrls(token.lastError || "")}
            </div>
            `
                : ""
            }
            ${
              !token.enable && disableReason
                ? `
            <div class="form-group compact">
                <label>⚠️ 禁用原因</label>
                <div class="token-disable-detail" style="padding: 0.5rem; background: var(--danger-bg, rgba(220,53,69,0.1)); border-radius: 6px; font-size: 0.85rem; color: var(--danger, #dc3545); word-break: break-all; max-height: 8em; overflow-y: auto;">${disableReason}${disableTimeStr ? '<br><span style="color: var(--text-light); font-size: 0.8rem;">禁用时间: ' + disableTimeStr + "</span>" : ""}</div>
                ${render403ActionUrls(token.disableReason || "")}
            </div>
            `
                : ""
            }
            <div class="modal-actions">
                <button class="btn btn-secondary" onclick="this.closest('.modal').remove()">取消</button>
                <button class="btn btn-success" onclick="saveTokenDetail('${safeTokenId}')">💾 保存</button>
            </div>
        </div>
    `;
  document.body.appendChild(modal);
  modal.onclick = (event) => {
    if (event.target === modal) modal.remove();
  };
}

async function saveTokenDetail(tokenId) {
  const projectId = document.getElementById("editProjectId").value.trim();
  const email = document.getElementById("editEmail").value.trim();

  showLoading("保存中...");
  try {
    const response = await authFetch(
      `/admin/tokens/${encodeURIComponent(tokenId)}`,
      {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ projectId, email }),
      },
    );

    const data = await response.json();
    hideLoading();
    if (data.success) {
      document.querySelector(".form-modal").remove();
      showToast("保存成功", "success");
      loadTokens();
    } else {
      showToast(data.message || "保存失败", "error");
    }
  } catch (error) {
    hideLoading();
    showToast("保存失败: " + error.message, "error");
  }
}

async function toggleToken(tokenId, enable) {
  const action = enable ? "启用" : "禁用";
  const confirmMsg = enable
    ? "确定要启用这个Token吗？\n系统将先验证凭证可用性，验证通过后才会启用。"
    : "确定要禁用这个Token吗？";
  const confirmed = await showConfirm(confirmMsg, `${action}确认`);
  if (!confirmed) return;

  showLoading(enable ? "正在验证凭证可用性..." : `正在${action}...`);
  try {
    const response = await authFetch(
      `/admin/tokens/${encodeURIComponent(tokenId)}`,
      {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(enable ? { enable, enableWithTest: true } : { enable }),
      },
    );

    const data = await response.json();
    hideLoading();
    if (data.success) {
      showToast(`已${action}`, "success");
      skipAnimation = true;
      loadTokens();
    } else {
      showToast(data.message || "操作失败", enable ? "warning" : "error");
      if (enable) {
        skipAnimation = true;
        loadTokens();
      }
    }
  } catch (error) {
    hideLoading();
    showToast("操作失败: " + error.message, "error");
  }
}

async function deleteToken(tokenId) {
  const confirmed = await showConfirm("删除后无法恢复，确定删除？", "⚠️ 删除确认");
  if (!confirmed) return;

  showLoading("正在删除...");
  try {
    const response = await authFetch(
      `/admin/tokens/${encodeURIComponent(tokenId)}`,
      {
        method: "DELETE",
      },
    );

    const data = await response.json();
    hideLoading();
    if (data.success) {
      showToast("已删除", "success");
      loadTokens();
    } else {
      showToast(data.message || "删除失败", "error");
    }
  } catch (error) {
    hideLoading();
    showToast("删除失败: " + error.message, "error");
  }
}

async function fetchProjectIdForManual() {
  const accessToken = document.getElementById("manualAccessToken").value.trim();
  const refreshToken = document.getElementById("manualRefreshToken").value.trim();

  if (!accessToken || !refreshToken) {
    showToast("请先填写 Access Token 和 Refresh Token", "warning");
    return;
  }

  const btn = document.getElementById("fetchProjectIdBtn");
  const originalText = btn.textContent;
  btn.disabled = true;
  btn.textContent = "⏳ 获取中...";

  try {
    const addResponse = await authFetch("/admin/tokens", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        access_token: accessToken,
        refresh_token: refreshToken,
        expires_in: 3599,
      }),
    });

    const addData = await addResponse.json();
    if (!addData.success) {
      throw new Error(addData.message || "添加 Token 失败");
    }

    const tokenId = addData.tokenId;

    const fetchResponse = await authFetch(
      `/admin/tokens/${encodeURIComponent(tokenId)}/fetch-project-id`,
      {
        method: "POST",
      },
    );

    const fetchData = await fetchResponse.json();

    if (fetchData.success && fetchData.projectId) {
      document.getElementById("manualProjectId").value = fetchData.projectId;
      showToast(`获取成功: ${fetchData.projectId}`, "success");

      await authFetch(`/admin/tokens/${encodeURIComponent(tokenId)}`, {
        method: "DELETE",
      });
    } else {
      await authFetch(`/admin/tokens/${encodeURIComponent(tokenId)}`, {
        method: "DELETE",
      });
      throw new Error(fetchData.message || "该账号无法获取 Project ID");
    }
  } catch (error) {
    showToast("获取失败: " + error.message, "error");
  } finally {
    btn.disabled = false;
    btn.textContent = originalText;
  }
}
