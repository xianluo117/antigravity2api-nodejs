if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initActionBarState);
} else {
  initActionBarState();
}
