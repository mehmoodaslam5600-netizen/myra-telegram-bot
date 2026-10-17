process.env.MYRA_SQLITE_STORE ||= "1";
process.env.MYRA_DB_PATH ||= "myra.db";
delete process.env.MYRA_MEM_STORE;

const { bot, BOT_TOKEN, ALLOWED_UPDATES, registerMenus } =
  await import("../.build-termux/bot.js");

if (!BOT_TOKEN || BOT_TOKEN === "missing-token") {
  console.error("BOT_TOKEN missing. Check your private .env file.");
  process.exit(1);
}

try {
  const me = await bot.api.getMe();
  console.log(`Telegram OK: @${me.username}`);

  await bot.api.deleteWebhook({ drop_pending_updates: false });
  await registerMenus();

  console.log("MYRA polling started. Press Ctrl+C to stop.");
  await bot.start({ allowed_updates: [...ALLOWED_UPDATES] });
} catch (error) {
  console.error("Termux runner error:", error);
  process.exitCode = 1;
}
