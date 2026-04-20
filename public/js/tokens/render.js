function formatTokenSubscriptionLabel(subscription) {
  const normalized = String(subscription || "free-tier")
    .trim()
    .toLowerCase();
  if (!normalized || normalized === "free-tier") return "Free";
  if (normalized.includes("ultra")) return "Ultra";
  if (
    normalized.includes("pro") ||
    normalized.includes("helium") ||
    normalized.includes("standard")
  ) {
    return "Pro";
  }
  return subscription || "Free";
}

function getTokenSubscriptionClass(subscription) {
  const normalized = String(subscription || "free-tier")
    .trim()
    .toLowerCase();
  if (!normalized || normalized === "free-tier") return "is-free";
  if (normalized.includes("ultra")) return "is-ultra";
  return "is-pro";
}

function formatTokenCreditsValue(credits) {
  if (credits === undefined || credits === null || credits === "") {
    return "未知";
  }
  const parsed = typeof credits === "number" ? credits : Number.parseFloat(credits);
  if (!Number.isFinite(parsed)) {
    return "未知";
  }
  return parsed.toLocaleString("zh-CN", { maximumFractionDigits: 2 });
}

function renderTokens(tokens) {
  if (tokens !== cachedTokens) {
    cachedTokens = tokens;
  }

  syncSelectedTokenIds();

  document.getElementById("totalTokens").textContent = tokens.length;
  document.getElementById("enabledTokens").textContent = tokens.filter(
    (token) => token.enable,
  ).length;
  document.getElementById("disabledTokens").textContent = tokens.filter(
    (token) => !token.enable,
  ).length;

  let filteredTokens = tokens;
  if (currentFilter === "enabled") {
    filteredTokens = tokens.filter((token) => token.enable);
  } else if (currentFilter === "disabled") {
    filteredTokens = tokens.filter((token) => !token.enable);
  }

  const tokenList = document.getElementById("tokenList");
  if (filteredTokens.length === 0) {
    const emptyText =
      currentFilter === "all"
        ? "暂无Token"
        : currentFilter === "enabled"
          ? "暂无启用的Token"
          : "暂无禁用的Token";
    const emptyHint =
      currentFilter === "all"
        ? "点击上方OAuth按钮添加Token"
        : '点击上方"总数"查看全部';
    tokenList.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">📦</div>
                <div class="empty-state-text">${emptyText}</div>
                <div class="empty-state-hint">${emptyHint}</div>
            </div>
        `;
    updateTokenBatchActionState();
    return;
  }

  tokenList.innerHTML = filteredTokens
    .map((token) => {
      const tokenId = token.id;
      const isRefreshing = refreshingTokens.has(tokenId);
      const isSelected = selectedTokenIds.has(tokenId);
      const cardId = tokenId.substring(0, 8);
      const subscription = token.sub || "free-tier";
      const creditsText = formatTokenCreditsValue(token.credits);
      const creditsTitle =
        token.credits === undefined || token.credits === null || token.credits === ""
          ? "积分信息暂未返回"
          : `积分余额: ${creditsText}`;

      const originalIndex = cachedTokens.findIndex((item) => item.id === token.id);
      const tokenNumber = originalIndex + 1;

      const safeTokenId = escapeJs(tokenId);
      const safeProjectId = escapeHtml(token.projectId || "");
      const safeEmail = escapeHtml(token.email || "");
      const safeProjectIdJs = escapeJs(token.projectId || "");
      const safeEmailJs = escapeJs(token.email || "");

      const disableReason = token.disableReason ? escapeHtml(token.disableReason) : "";
      const disableTimeStr = token.disableTime
        ? new Date(token.disableTime).toLocaleString("zh-CN")
        : "";

      return `
        <div class="token-card ${!token.enable ? "disabled" : ""} ${isRefreshing ? "refreshing" : ""} ${skipAnimation ? "no-animation" : ""} ${isSelected ? "selected" : ""}" id="card-${escapeHtml(cardId)}">
            <div class="token-header">
                <div class="token-header-left">
                    <label class="token-select-wrap" title="选择此 Token 进行批量操作">
                        <input type="checkbox" class="token-select-checkbox" ${isSelected ? "checked" : ""} onclick="toggleTokenSelection('${safeTokenId}', this.checked, event)">
                    </label>
                    <span class="status ${token.enable ? "enabled" : "disabled"}">
                        ${token.enable ? "✅ 启用" : "❌ 禁用"}
                    </span>
                    <button class="btn-icon token-refresh-btn ${isRefreshing ? "loading" : ""}" id="refresh-btn-${escapeHtml(cardId)}" onclick="manualRefreshToken('${safeTokenId}')" title="刷新Token" ${isRefreshing ? "disabled" : ""}>🔄</button>
                </div>
                <div class="token-header-right">
                    <button class="btn-icon" onclick="showTokenDetail('${safeTokenId}')" title="编辑">✏️</button>
                    <span class="token-id">#${tokenNumber}</span>
                </div>
            </div>
            ${!token.enable && disableReason ? `<div class="token-disable-reason" title="${disableTimeStr ? "禁用时间: " + disableTimeStr : ""}">⚠️ ${disableReason}${disableTimeStr ? " (" + disableTimeStr + ")" : ""}</div>${render403ActionUrls(token.disableReason || "")}` : ""}
            <div class="token-meta-row">
                <span class="token-tier-badge ${getTokenSubscriptionClass(subscription)}" title="订阅等级: ${escapeHtml(subscription)}">⭐ ${escapeHtml(formatTokenSubscriptionLabel(subscription))}</span>
                <span class="token-credit-badge ${token.credits > 0 ? "has-credits" : ""}" title="${escapeHtml(creditsTitle)}">💳 ${escapeHtml(creditsText)}</span>
            </div>
            <div class="token-info">
                <div class="info-row editable sensitive-row" onclick="editField(event, '${safeTokenId}', 'projectId', '${safeProjectIdJs}')" title="点击编辑">
                    <span class="info-label">📦</span>
                    <span class="info-value sensitive-info">${safeProjectId || "点击设置"}</span>
                    <span class="info-edit-icon">✏️</span>
                    <button class="btn btn-xs btn-info fetch-project-btn" onclick="fetchProjectId(event, '${safeTokenId}')" title="从API获取Project ID">🔍</button>
                </div>
                <div class="info-row editable sensitive-row" onclick="editField(event, '${safeTokenId}', 'email', '${safeEmailJs}')" title="点击编辑">
                    <span class="info-label">📧</span>
                    <span class="info-value sensitive-info">${safeEmail || "点击设置"}</span>
                    <span class="info-edit-icon">✏️</span>
                </div>
            </div>
            <div class="token-id-row" title="Token ID: ${escapeHtml(tokenId)}">
                <span class="token-id-label">🔑</span>
                <span class="token-id-value">${escapeHtml(tokenId.length > 24 ? tokenId.substring(0, 12) + "..." + tokenId.substring(tokenId.length - 8) : tokenId)}</span>
            </div>
            <div class="token-quota-inline" id="quota-inline-${escapeHtml(cardId)}">
                <div class="quota-inline-header" onclick="toggleQuotaExpand('${escapeJs(cardId)}', '${safeTokenId}')">
                    <span class="quota-inline-summary" id="quota-summary-${escapeHtml(cardId)}">📊 加载中...</span>
                    <span class="quota-inline-toggle" id="quota-toggle-${escapeHtml(cardId)}">▼</span>
                </div>
                <div class="quota-inline-detail hidden" id="quota-detail-${escapeHtml(cardId)}"></div>
            </div>
            <div class="token-actions">
                <button class="btn btn-info btn-xs" onclick="showQuotaModal('${safeTokenId}')" title="查看额度">📊 详情</button>
                <button class="btn ${token.enable ? "btn-warning" : "btn-success"} btn-xs" onclick="toggleToken('${safeTokenId}', ${!token.enable})" title="${token.enable ? "禁用" : "启用"}">
                    ${token.enable ? "⏸️ 禁用" : "▶️ 启用"}
                </button>
                <button class="btn btn-danger btn-xs" onclick="deleteToken('${safeTokenId}')" title="删除">🗑️ 删除</button>
            </div>
        </div>
    `;
    })
    .join("");

  filteredTokens.forEach((token) => {
    loadTokenQuotaSummary(token.id);
  });

  updateSensitiveInfoDisplay();
  updateTokenBatchActionState();
  skipAnimation = false;
}
