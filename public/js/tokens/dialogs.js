function showPasswordPrompt(message) {
  return new Promise((resolve) => {
    const modal = document.createElement("div");
    modal.className = "modal form-modal";
    modal.innerHTML = `
            <div class="modal-content">
                <div class="modal-title">🔐 密码验证</div>
                <p>${message}</p>
                <div class="form-group">
                    <input type="password" id="promptPassword" placeholder="请输入密码">
                </div>
                <div class="modal-actions">
                    <button class="btn btn-secondary" id="promptCancelBtn">取消</button>
                    <button class="btn btn-success" id="promptConfirmBtn">确认</button>
                </div>
            </div>
        `;
    document.body.appendChild(modal);

    const passwordInput = document.getElementById("promptPassword");
    const confirmBtn = document.getElementById("promptConfirmBtn");
    const cancelBtn = document.getElementById("promptCancelBtn");

    const cleanup = () => {
      confirmBtn.removeEventListener("click", handleConfirm);
      cancelBtn.removeEventListener("click", handleCancel);
      passwordInput.removeEventListener("keydown", handleKeydown);
      modal.removeEventListener("click", handleModalClick);
      modal.remove();
    };

    const handleConfirm = () => {
      const password = passwordInput.value;
      cleanup();
      resolve(password || null);
    };

    const handleCancel = () => {
      cleanup();
      resolve(null);
    };

    const handleKeydown = (e) => {
      if (e.key === "Enter") {
        handleConfirm();
      } else if (e.key === "Escape") {
        handleCancel();
      }
    };

    const handleModalClick = (e) => {
      if (e.target === modal) {
        cleanup();
        resolve(null);
      }
    };

    confirmBtn.addEventListener("click", handleConfirm);
    cancelBtn.addEventListener("click", handleCancel);
    passwordInput.addEventListener("keydown", handleKeydown);
    modal.addEventListener("click", handleModalClick);

    passwordInput.focus();
  });
}
