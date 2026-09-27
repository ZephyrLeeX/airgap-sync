// Refresh one server-rendered snapshot every 20 seconds.
if (location.pathname === "/") {
  setInterval(() => location.reload(), 20000);
}
