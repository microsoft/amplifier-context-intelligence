// David's exact theme detection approach
(function () {
  document.documentElement.classList.toggle(
    'dark',
    localStorage.theme === 'dark' ||
      (localStorage.theme !== 'light' &&
        window.matchMedia('(prefers-color-scheme: dark)').matches)
  );
})();

function toggleTheme() {
  const isDark = document.documentElement.classList.toggle('dark');
  localStorage.theme = isDark ? 'dark' : 'light';
  const btn = document.getElementById('theme-toggle');
  if (btn) btn.textContent = isDark ? '☀' : '☾';
}
