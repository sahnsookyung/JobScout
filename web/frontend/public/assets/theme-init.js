(function () {
  try {
    var saved = localStorage.getItem('jobscout-theme');
    var prefersDark =
      globalThis.matchMedia &&
      globalThis.matchMedia('(prefers-color-scheme: dark)').matches;
    var theme = saved || (prefersDark ? 'dark' : 'light');
    document.documentElement.dataset.theme = theme;
    var themeColor = document.querySelector('meta[name="theme-color"][data-theme-color]');
    if (themeColor) {
      themeColor.setAttribute('content', theme === 'dark' ? '#15130F' : '#F2EEE5');
    }
  } catch {
    document.documentElement.dataset.theme = 'light';
  }
})();
