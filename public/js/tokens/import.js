function showImportUploadModal() {
  const modal = document.createElement("div");
  modal.className = "modal form-modal";
  modal.id = "importUploadModal";
  modal.innerHTML = `
        <div class="modal-content modal-lg">
            <div class="modal-title">📥 添加/导入 Token</div>
            
            <div class="import-tabs">
                <button class="import-tab active" data-tab="file" onclick="switchImportTab('file')">📁 文件上传</button>
                <button class="import-tab" data-tab="json" onclick="switchImportTab('json')">📝 JSON导入</button>
                <button class="import-tab" data-tab="manual" onclick="switchImportTab('manual')">✏️ 手动填入</button>
            </div>
            
            <div class="import-tab-content" id="importTabFile">
                <div class="import-dropzone" id="importDropzone">
                    <div class="dropzone-icon">📁</div>
                    <div class="dropzone-text">拖拽文件到此处</div>
                    <div class="dropzone-hint">或点击选择文件</div>
                    <input type="file" id="importFileInput" accept=".json" style="display: none;">
                </div>
                <div class="import-file-info hidden" id="importFileInfo">
                    <div class="file-info-icon">📄</div>
                    <div class="file-info-details">
                        <div class="file-info-name" id="importFileName">-</div>
                        <div class="file-info-meta" id="importFileMeta">-</div>
                    </div>
                    <button class="btn btn-xs btn-secondary" onclick="clearImportFile()">✕</button>
                </div>
            </div>
            
            <div class="import-tab-content hidden" id="importTabJson">
                <div class="form-group">
                    <label>📝 粘贴 JSON 内容</label>
                    <textarea id="importJsonInput" rows="8" placeholder='{"tokens": [...], "exportTime": "..."}'></textarea>
                </div>
                <div class="import-json-actions">
                    <button class="btn btn-sm btn-info" onclick="parseImportJson()">🔍 解析 JSON</button>
                    <span class="import-json-status" id="importJsonStatus"></span>
                </div>
            </div>
            
            <div class="import-tab-content hidden" id="importTabManual">
                <div class="form-group">
                    <label>🔑 Access Token <span style="color: var(--danger);">*</span></label>
                    <input type="text" id="manualAccessToken" placeholder="Access Token (必填)" autocomplete="off">
                </div>
                <div class="form-group">
                    <label>🔄 Refresh Token <span style="color: var(--danger);">*</span></label>
                    <input type="text" id="manualRefreshToken" placeholder="Refresh Token (必填)" autocomplete="off">
                </div>
                <div class="form-group">
                    <label>📁 Project ID</label>
                    <div style="display: flex; gap: 0.5rem;">
                        <input type="text" id="manualProjectId" placeholder="Project ID (可选，留空则自动获取)" style="flex: 1;" autocomplete="off">
                        <button class="btn btn-sm btn-info" id="fetchProjectIdBtn" onclick="fetchProjectIdForManual()" style="white-space: nowrap;">🔍 自动获取</button>
                    </div>
                    <p style="font-size: 0.75rem; color: var(--text-light); margin-top: 0.25rem;">💡 可以手动填写，或填写 Token 后点击“自动获取”</p>
                </div>
                <div class="form-group">
                    <label>⏱️ 有效期(秒)</label>
                    <input type="number" id="manualExpiresIn" placeholder="有效期(秒)" value="3599" autocomplete="off">
                </div>
                <p style="font-size: 0.8rem; color: var(--text-light); margin-bottom: 0.5rem;">💡 有效期默认3599秒(约1小时)，手动填入不需要密码验证</p>
            </div>
            
            <div class="form-group" id="importModeGroup">
                <label>导入模式</label>
                <select id="importMode">
                    <option value="merge">合并（保留现有，添加新的）</option>
                    <option value="replace">替换（清空现有，导入新的）</option>
                </select>
            </div>
            
            <div class="form-group" id="importPasswordGroup">
                <label>🔐 管理员密码</label>
                <input type="password" id="importPassword" placeholder="请输入管理员密码验证">
            </div>
            
            <div class="modal-actions">
                <button class="btn btn-secondary" onclick="closeImportModal()">取消</button>
                <button class="btn btn-success" id="confirmImportBtn" onclick="confirmImportFromModal()" disabled>✅ 确认</button>
            </div>
        </div>
    `;
  document.body.appendChild(modal);

  currentImportTab = "file";

  const dropzone = document.getElementById("importDropzone");
  const fileInput = document.getElementById("importFileInput");
  const manualAccessToken = document.getElementById("manualAccessToken");
  const manualRefreshToken = document.getElementById("manualRefreshToken");

  const cleanupDropzone =
    typeof wireJsonFileDropzone === "function"
      ? wireJsonFileDropzone({
          dropzone,
          fileInput,
          onFile: (file) => handleImportFile(file),
          onError: (message) => showToast(message, "warning"),
        })
      : null;
  const cleanupBackdrop =
    typeof wireModalBackdropClose === "function"
      ? wireModalBackdropClose(modal, closeImportModal)
      : null;

  const handlers = {
    updateManualBtnState: () => {
      if (currentImportTab === "manual") {
        const confirmBtn = document.getElementById("confirmImportBtn");
        confirmBtn.disabled =
          !manualAccessToken.value.trim() || !manualRefreshToken.value.trim();
      }
    },
  };

  importModalHandlers = {
    modal,
    dropzone,
    fileInput,
    manualAccessToken,
    manualRefreshToken,
    handlers,
    cleanup: () => {
      try {
        cleanupDropzone && cleanupDropzone();
      } catch {
        /* ignore */
      }
      try {
        cleanupBackdrop && cleanupBackdrop();
      } catch {
        /* ignore */
      }
    },
  };

  manualAccessToken.addEventListener("input", handlers.updateManualBtnState);
  manualRefreshToken.addEventListener("input", handlers.updateManualBtnState);
}

function switchImportTab(tab) {
  currentImportTab = tab;

  document
    .querySelectorAll(".import-tab")
    .forEach((item) => item.classList.remove("active"));
  document
    .querySelector(`.import-tab[data-tab="${tab}"]`)
    .classList.add("active");

  document.getElementById("importTabFile").classList.toggle("hidden", tab !== "file");
  document.getElementById("importTabJson").classList.toggle("hidden", tab !== "json");
  document.getElementById("importTabManual").classList.toggle("hidden", tab !== "manual");

  const importModeGroup = document.getElementById("importModeGroup");
  const importPasswordGroup = document.getElementById("importPasswordGroup");
  const confirmBtn = document.getElementById("confirmImportBtn");

  if (tab === "manual") {
    importModeGroup.classList.add("hidden");
    importPasswordGroup.classList.add("hidden");
    const accessToken = document.getElementById("manualAccessToken").value.trim();
    const refreshToken = document.getElementById("manualRefreshToken").value.trim();
    confirmBtn.disabled = !accessToken || !refreshToken;
    confirmBtn.textContent = "✅ 添加";
  } else {
    importModeGroup.classList.remove("hidden");
    importPasswordGroup.classList.remove("hidden");
    confirmBtn.textContent = "✅ 确认导入";

    if (tab === "file") {
      document.getElementById("importJsonInput").value = "";
      document.getElementById("importJsonStatus").textContent = "";
      document.getElementById("manualAccessToken").value = "";
      document.getElementById("manualRefreshToken").value = "";
      document.getElementById("manualExpiresIn").value = "3599";
      confirmBtn.disabled = !pendingImportData;
    } else if (tab === "json") {
      clearImportFile();
      document.getElementById("manualAccessToken").value = "";
      document.getElementById("manualRefreshToken").value = "";
      document.getElementById("manualExpiresIn").value = "3599";
      confirmBtn.disabled = !pendingImportData;
    }
  }
}

function findFieldByKeyword(obj, keyword) {
  if (!obj || typeof obj !== "object") return undefined;
  const lowerKeyword = keyword.toLowerCase();
  for (const key of Object.keys(obj)) {
    if (key.toLowerCase().includes(lowerKeyword)) {
      return obj[key];
    }
  }
  return undefined;
}

function smartParseToken(rawToken) {
  if (!rawToken || typeof rawToken !== "object") return null;

  const refresh_token = findFieldByKeyword(rawToken, "refresh");
  const projectId = findFieldByKeyword(rawToken, "project");

  if (!refresh_token || !projectId) return null;

  const token = { refresh_token, projectId };

  const access_token = findFieldByKeyword(rawToken, "access");
  const email =
    findFieldByKeyword(rawToken, "email") ||
    findFieldByKeyword(rawToken, "mail");
  const expires_in = findFieldByKeyword(rawToken, "expire");
  const enable = findFieldByKeyword(rawToken, "enable");
  const timestamp =
    findFieldByKeyword(rawToken, "time") ||
    findFieldByKeyword(rawToken, "stamp");
  const hasQuota = findFieldByKeyword(rawToken, "quota");

  if (access_token) token.access_token = access_token;
  if (email) token.email = email;
  if (expires_in !== undefined) token.expires_in = parseInt(expires_in, 10) || 3599;
  if (enable !== undefined) {
    token.enable = enable === true || enable === "true" || enable === 1;
  }
  if (timestamp) {
    token.timestamp =
      typeof timestamp === "number" ? timestamp : new Date(timestamp).getTime();
  }
  if (hasQuota !== undefined) {
    token.hasQuota = hasQuota === true || hasQuota === "true" || hasQuota === 1;
  }

  return token;
}

function smartParseImportData(jsonText) {
  let data;
  let cleanText = jsonText.trim();

  cleanText = cleanText.replace(/,(\s*[}\]])/g, "$1");

  try {
    data = JSON.parse(cleanText);
  } catch (error) {
    try {
      const arrayText = "[" + cleanText.replace(/\}\s*\{/g, "},{") + "]";
      data = JSON.parse(arrayText);
    } catch {
      return { success: false, message: `JSON 解析错误: ${error.message}` };
    }
  }

  let tokensArray = [];
  if (Array.isArray(data)) {
    tokensArray = data;
  } else if (typeof data === "object" && data !== null) {
    for (const key of Object.keys(data)) {
      if (Array.isArray(data[key])) {
        tokensArray = data[key];
        break;
      }
    }
    if (tokensArray.length === 0) {
      const single = smartParseToken(data);
      if (single) tokensArray = [data];
    }
  }

  if (tokensArray.length === 0) {
    return {
      success: false,
      message: "未找到有效数据，请确保包含 refresh_token 和 projectId",
    };
  }

  const validTokens = [];
  let invalidCount = 0;
  for (const raw of tokensArray) {
    const parsed = smartParseToken(raw);
    if (parsed) {
      validTokens.push(parsed);
    } else {
      invalidCount++;
    }
  }

  if (validTokens.length === 0) {
    return {
      success: false,
      message: `所有 ${tokensArray.length} 条数据都缺少必需字段 (refresh_token 和 projectId)`,
    };
  }

  const message =
    invalidCount > 0
      ? `解析成功：${validTokens.length} 个有效，${invalidCount} 个无效`
      : `解析成功：${validTokens.length} 个 Token`;

  return { success: true, tokens: validTokens, message };
}

function parseImportJson() {
  const jsonInput = document.getElementById("importJsonInput");
  const statusEl = document.getElementById("importJsonStatus");
  const confirmBtn = document.getElementById("confirmImportBtn");

  const jsonText = jsonInput.value.trim();
  if (!jsonText) {
    statusEl.textContent = "❌ 请输入 JSON 内容";
    statusEl.className = "import-json-status error";
    pendingImportData = null;
    confirmBtn.disabled = true;
    return;
  }

  const result = smartParseImportData(jsonText);

  if (result.success) {
    pendingImportData = { tokens: result.tokens };
    statusEl.textContent = `✅ ${result.message}`;
    statusEl.className = "import-json-status success";
    confirmBtn.disabled = false;
  } else {
    statusEl.textContent = `❌ ${result.message}`;
    statusEl.className = "import-json-status error";
    pendingImportData = null;
    confirmBtn.disabled = true;
  }
}

async function handleImportFile(file) {
  try {
    const text = await file.text();
    const result = smartParseImportData(text);

    if (!result.success) {
      showToast(result.message, "error");
      return;
    }

    pendingImportData = { tokens: result.tokens };

    const dropzone = document.getElementById("importDropzone");
    const fileInfo = document.getElementById("importFileInfo");
    const fileName = document.getElementById("importFileName");
    const fileMeta = document.getElementById("importFileMeta");
    const confirmBtn = document.getElementById("confirmImportBtn");

    dropzone.classList.add("hidden");
    fileInfo.classList.remove("hidden");
    fileName.textContent = file.name;
    fileMeta.textContent = result.message;
    confirmBtn.disabled = false;
  } catch (error) {
    showToast("读取文件失败: " + error.message, "error");
  }
}

function clearImportFile() {
  pendingImportData = null;

  const dropzone = document.getElementById("importDropzone");
  const fileInfo = document.getElementById("importFileInfo");
  const fileInput = document.getElementById("importFileInput");
  const confirmBtn = document.getElementById("confirmImportBtn");

  dropzone.classList.remove("hidden");
  fileInfo.classList.add("hidden");
  fileInput.value = "";
  confirmBtn.disabled = true;
}

function closeImportModal() {
  if (importModalHandlers) {
    const { manualAccessToken, manualRefreshToken, handlers, cleanup } =
      importModalHandlers;

    if (typeof cleanup === "function") {
      try {
        cleanup();
      } catch {
        /* ignore */
      }
    } else {
      const { modal, dropzone, fileInput } = importModalHandlers;
      if (dropzone && handlers) {
        if (handlers.dropzoneClick) {
          dropzone.removeEventListener("click", handlers.dropzoneClick);
        }
        if (handlers.dragover) {
          dropzone.removeEventListener("dragover", handlers.dragover);
        }
        if (handlers.dragleave) {
          dropzone.removeEventListener("dragleave", handlers.dragleave);
        }
        if (handlers.drop) {
          dropzone.removeEventListener("drop", handlers.drop);
        }
      }
      if (fileInput && handlers?.fileChange) {
        fileInput.removeEventListener("change", handlers.fileChange);
      }
      if (modal && handlers?.modalClick) {
        modal.removeEventListener("click", handlers.modalClick);
      }
    }

    if (manualAccessToken && handlers?.updateManualBtnState) {
      manualAccessToken.removeEventListener(
        "input",
        handlers.updateManualBtnState,
      );
    }
    if (manualRefreshToken && handlers?.updateManualBtnState) {
      manualRefreshToken.removeEventListener(
        "input",
        handlers.updateManualBtnState,
      );
    }

    importModalHandlers = null;
  }

  const modal = document.getElementById("importUploadModal");
  if (modal) {
    modal.remove();
  }
  pendingImportData = null;
}

async function confirmImportFromModal() {
  if (currentImportTab === "manual") {
    const accessToken = document.getElementById("manualAccessToken").value.trim();
    const refreshToken = document.getElementById("manualRefreshToken").value.trim();
    const projectId = document.getElementById("manualProjectId").value.trim();
    const expiresIn =
      parseInt(document.getElementById("manualExpiresIn").value, 10) || 3599;

    if (!accessToken || !refreshToken) {
      showToast("请填写完整的Token信息", "warning");
      return;
    }

    showLoading("正在添加Token...");
    try {
      const tokenData = {
        access_token: accessToken,
        refresh_token: refreshToken,
        expires_in: expiresIn,
      };
      if (projectId) {
        tokenData.projectId = projectId;
      }
      const response = await authFetch("/admin/tokens", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(tokenData),
      });

      const data = await response.json();
      hideLoading();

      if (data.success) {
        closeImportModal();
        const toastType = data.disabled ? "warning" : "success";
        showToast(data.message || "Token添加成功", toastType);
        loadTokens();
      } else {
        showToast(data.message || "添加失败", "error");
      }
    } catch (error) {
      hideLoading();
      showToast("添加失败: " + error.message, "error");
    }
    return;
  }

  if (!pendingImportData) {
    showToast("请先选择文件或解析JSON", "warning");
    return;
  }

  const mode = document.getElementById("importMode").value;
  const password = document.getElementById("importPassword").value;

  if (!password) {
    showToast("请输入管理员密码", "warning");
    return;
  }

  showLoading("正在导入...");
  try {
    const response = await authFetch("/admin/tokens/import", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ password, data: pendingImportData, mode }),
    });

    const data = await response.json();
    hideLoading();

    if (data.success) {
      closeImportModal();
      showToast(data.message, "success");
      loadTokens();
    } else if (response.status === 403) {
      showToast("密码错误，请重新输入", "error");
    } else {
      showToast(data.message || "导入失败", "error");
    }
  } catch (error) {
    hideLoading();
    showToast("导入失败: " + error.message, "error");
  }
}
