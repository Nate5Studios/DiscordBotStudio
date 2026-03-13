import asyncio
import json
import os
import queue
import threading
import tkinter as tk
from datetime import datetime, timedelta
from tkinter import filedialog, messagebox, simpledialog, ttk

import discord

BOT_FILE = "bots.txt"
CHANNEL_FILE = "channels.txt"
MEMBER_FILE = "members.txt"
SETTINGS_FILE = "settings.json"
LOG_FOLDER = "logs"
MAX_LOG_PREVIEW_LINES = 200

THEMES = {
    "Midnight": {
        "app_bg": "#0E1524",
        "card_bg": "#18243A",
        "text_main": "#E8EEF8",
        "text_sub": "#9CB0CF",
        "list_bg": "#0F182A",
        "select_bg": "#2E4D83",
    },
    "Ocean": {
        "app_bg": "#0B1E24",
        "card_bg": "#17353F",
        "text_main": "#E7FAFF",
        "text_sub": "#9CD3DC",
        "list_bg": "#102A30",
        "select_bg": "#2E7886",
    },
    "Sunset": {
        "app_bg": "#2A1B1B",
        "card_bg": "#4A2A2A",
        "text_main": "#FFF0E8",
        "text_sub": "#F3BAA8",
        "list_bg": "#3A2323",
        "select_bg": "#A8553A",
    },
}

DEFAULT_TEMPLATES = [
    "Hello everyone",
    "Status update: all systems online.",
    "Thanks for the report, checking now.",
]

DEFAULT_EMBED_PRESETS = [
    {
        "name": "Status",
        "title": "Status Update",
        "description": "All systems are operating normally.",
        "color": "#4A90E2",
        "footer": "Discord Bot Studio",
    },
    {
        "name": "Alert",
        "title": "Attention",
        "description": "Please review the latest update.",
        "color": "#E67E22",
        "footer": "Action may be required",
    },
]

MAX_ACTIVITY_ITEMS = 80
SCHEDULE_RETRY_DELAY_SECONDS = 30
MAX_BULK_HISTORY_ITEMS = 80


def ensure_storage():
    if not os.path.exists(LOG_FOLDER):
        os.makedirs(LOG_FOLDER)
    for file_name in [BOT_FILE, CHANNEL_FILE, MEMBER_FILE]:
        if not os.path.exists(file_name):
            open(file_name, "w", encoding="utf8").close()
    if not os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE, "w", encoding="utf8") as f:
            json.dump({}, f)


def load_kv_file(path):
    data = {}
    with open(path, "r", encoding="utf8") as f:
        for line in f:
            if "|" not in line:
                continue
            key, value = line.strip().split("|", 1)
            data[key] = value
    return data


def save_kv_file(path, data):
    with open(path, "w", encoding="utf8") as f:
        for key, value in data.items():
            f.write(f"{key}|{value}\n")


def load_settings():
    try:
        with open(SETTINGS_FILE, "r", encoding="utf8") as f:
            content = json.load(f)
            return content if isinstance(content, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def save_settings(settings):
    with open(SETTINGS_FILE, "w", encoding="utf8") as f:
        json.dump(settings, f, indent=2)


def safe_log_name(channel_name):
    cleaned = "".join(ch for ch in channel_name if ch.isalnum() or ch in ("-", "_"))
    return cleaned or "default"


def write_log_line(channel_name, text):
    path = os.path.join(LOG_FOLDER, f"log_{safe_log_name(channel_name)}.txt")
    with open(path, "a", encoding="utf8") as f:
        f.write(text + "\n")


def read_log_lines(channel_name):
    path = os.path.join(LOG_FOLDER, f"log_{safe_log_name(channel_name)}.txt")
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf8") as f:
        return [line.rstrip("\n") for line in f.readlines()]


def mask_token(token):
    if len(token) <= 8:
        return "*" * len(token)
    return token[:4] + ("*" * (len(token) - 8)) + token[-4:]


def serialize_schedule_job(job):
    return {
        "id": job["id"],
        "bot": job["bot"],
        "channel_name": job["channel_name"],
        "channel_id": job["channel_id"],
        "text": job["text"],
        "run_at": job["run_at"].isoformat() + "Z",
        "interval_seconds": job.get("interval_seconds", 0),
        "remaining_runs": job.get("remaining_runs", 1),
        "paused": bool(job.get("paused", False)),
        "max_retries": int(job.get("max_retries", 5)),
        "run_count": int(job.get("run_count", 0)),
        "fail_count": int(job.get("fail_count", 0)),
        "retry_count": int(job.get("retry_count", 0)),
        "last_run_at": str(job.get("last_run_at", "")),
        "last_result": str(job.get("last_result", "pending")),
        "last_error": str(job.get("last_error", "")),
    }


def deserialize_schedule_jobs(items):
    jobs = []
    if not isinstance(items, list):
        return jobs
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            run_at_raw = str(item.get("run_at", "")).replace("Z", "")
            run_at = datetime.fromisoformat(run_at_raw)
            jobs.append(
                {
                    "id": int(item.get("id", 0)),
                    "bot": str(item.get("bot", "")),
                    "channel_name": str(item.get("channel_name", "")),
                    "channel_id": str(item.get("channel_id", "")),
                    "text": str(item.get("text", "")),
                    "run_at": run_at,
                    "interval_seconds": max(0, int(item.get("interval_seconds", 0))),
                    "remaining_runs": max(1, int(item.get("remaining_runs", 1))),
                    "paused": bool(item.get("paused", False)),
                    "max_retries": max(0, int(item.get("max_retries", 5))),
                    "run_count": max(0, int(item.get("run_count", 0))),
                    "fail_count": max(0, int(item.get("fail_count", 0))),
                    "retry_count": max(0, int(item.get("retry_count", 0))),
                    "last_run_at": str(item.get("last_run_at", "")),
                    "last_result": str(item.get("last_result", "pending")),
                    "last_error": str(item.get("last_error", "")),
                }
            )
        except (TypeError, ValueError):
            continue
    return jobs


def normalize_templates(items):
    templates = []
    if not isinstance(items, list):
        items = DEFAULT_TEMPLATES[:]
    for item in items:
        if isinstance(item, str):
            text = item.strip()
            if text:
                templates.append({"text": text, "bot": "", "channel": ""})
        elif isinstance(item, dict):
            text = str(item.get("text", "")).strip()
            if text:
                templates.append(
                    {
                        "text": text,
                        "bot": str(item.get("bot", "")).strip(),
                        "channel": str(item.get("channel", "")).strip(),
                    }
                )
    return templates or [{"text": item, "bot": "", "channel": ""} for item in DEFAULT_TEMPLATES]


def template_label(template):
    bot = template.get("bot") or "AnyBot"
    channel = template.get("channel") or "AnyChannel"
    text = template.get("text", "")
    preview = text[:46].replace("\n", " ")
    return f"[{bot} / {channel}] {preview}"


def normalize_embed_presets(items):
    presets = []
    if not isinstance(items, list):
        items = DEFAULT_EMBED_PRESETS[:]
    for item in items:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        title = str(item.get("title", "")).strip()
        description = str(item.get("description", "")).strip()
        color = str(item.get("color", "#4A90E2")).strip() or "#4A90E2"
        footer = str(item.get("footer", "")).strip()
        if name:
            presets.append(
                {
                    "name": name,
                    "title": title,
                    "description": description,
                    "color": color,
                    "footer": footer,
                }
            )
    return presets or [dict(item) for item in DEFAULT_EMBED_PRESETS]


def embed_preset_label(preset):
    return f"{preset.get('name', 'Preset')} | {preset.get('title', '')[:28]}"


class DiscordRuntime:
    def __init__(self, event_queue):
        self.event_queue = event_queue
        self.loop = None
        self.thread = None
        self.ready = threading.Event()
        self.clients = {}
        self.client_tasks = {}
        self.sender_tasks = {}
        self.queues = {}
        self.start()

    def start(self):
        def runner():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.ready.set()
            self.loop.run_forever()

        self.thread = threading.Thread(target=runner, daemon=True)
        self.thread.start()
        self.ready.wait(timeout=5)

    def post_event(self, kind, payload):
        self.event_queue.put({"kind": kind, "payload": payload})

    def run_coro(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def is_logged_in(self, bot_name):
        return bot_name in self.clients

    async def _close_bot(self, bot_name):
        sender_task = self.sender_tasks.pop(bot_name, None)
        if sender_task:
            sender_task.cancel()
        client = self.clients.pop(bot_name, None)
        if client:
            try:
                await client.close()
            except Exception:
                pass
        client_task = self.client_tasks.pop(bot_name, None)
        if client_task:
            client_task.cancel()
        self.queues.pop(bot_name, None)

    async def _sender_loop(self, bot_name):
        while True:
            message, channel_id = await self.queues[bot_name].get()
            client = self.clients.get(bot_name)
            if not client:
                continue
            try:
                channel_obj = client.get_channel(int(channel_id))
                if channel_obj is None:
                    channel_obj = await client.fetch_channel(int(channel_id))
                await channel_obj.send(message)
            except discord.HTTPException:
                self.post_event("system", f"[{bot_name}] Rate limited, retrying in 2s")
                await asyncio.sleep(2)
                await self.queues[bot_name].put((message, channel_id))
            except Exception as exc:
                self.post_event("system", f"[{bot_name}] Send failed: {exc}")

    async def _login(self, bot_name, token):
        if bot_name in self.clients:
            await self._close_bot(bot_name)

        intents = discord.Intents.default()
        intents.message_content = True
        client = discord.Client(intents=intents)
        self.clients[bot_name] = client
        self.queues[bot_name] = asyncio.Queue()

        @client.event
        async def on_ready():
            self.post_event("ready", {"bot_name": bot_name, "user": str(client.user)})

        @client.event
        async def on_message(message):
            if message.author.bot:
                return
            self.post_event(
                "incoming",
                {
                    "bot_name": bot_name,
                    "channel_id": str(message.channel.id),
                    "text": f"{message.author}: {message.content}",
                },
            )

        async def client_runner():
            try:
                await client.start(token)
            except Exception as exc:
                self.post_event("system", f"[{bot_name}] Login failed: {exc}")
                await self._close_bot(bot_name)

        self.sender_tasks[bot_name] = asyncio.create_task(self._sender_loop(bot_name))
        self.client_tasks[bot_name] = asyncio.create_task(client_runner())
        self.post_event("system", f"[{bot_name}] Login started")

    def login(self, bot_name, token):
        return self.run_coro(self._login(bot_name, token))

    async def _logout(self, bot_name):
        if bot_name not in self.clients:
            self.post_event("system", f"[{bot_name}] Not currently logged in")
            return
        await self._close_bot(bot_name)
        self.post_event("system", f"[{bot_name}] Logged out")

    def logout(self, bot_name):
        return self.run_coro(self._logout(bot_name))

    async def _send_message(self, bot_name, text, channel_id):
        if bot_name not in self.queues:
            self.post_event("system", f"[{bot_name}] Not logged in")
            return
        await self.queues[bot_name].put((text, channel_id))

    def send_message(self, bot_name, text, channel_id):
        return self.run_coro(self._send_message(bot_name, text, channel_id))

    async def _send_embed(self, bot_name, channel_id, title, description, color_value, footer):
        client = self.clients.get(bot_name)
        if not client:
            self.post_event("system", f"[{bot_name}] Not logged in")
            return
        try:
            channel_obj = client.get_channel(int(channel_id))
            if channel_obj is None:
                channel_obj = await client.fetch_channel(int(channel_id))
            embed = discord.Embed(title=title or None, description=description or None, color=color_value)
            if footer:
                embed.set_footer(text=footer)
            await channel_obj.send(embed=embed)
        except Exception as exc:
            self.post_event("system", f"[{bot_name}] Embed send failed: {exc}")

    def send_embed(self, bot_name, channel_id, title, description, color_value, footer):
        return self.run_coro(self._send_embed(bot_name, channel_id, title, description, color_value, footer))

    async def _send_file(self, bot_name, channel_id, file_path, caption):
        client = self.clients.get(bot_name)
        if not client:
            self.post_event("system", f"[{bot_name}] Not logged in")
            return
        try:
            channel_obj = client.get_channel(int(channel_id))
            if channel_obj is None:
                channel_obj = await client.fetch_channel(int(channel_id))
            discord_file = discord.File(file_path)
            await channel_obj.send(content=caption or None, file=discord_file)
        except Exception as exc:
            self.post_event("system", f"[{bot_name}] File send failed: {exc}")

    def send_file(self, bot_name, channel_id, file_path, caption):
        return self.run_coro(self._send_file(bot_name, channel_id, file_path, caption))

    async def _fetch_channels(self, bot_name):
        client = self.clients.get(bot_name)
        if not client or not client.is_ready():
            return []
        result = []
        for guild in client.guilds:
            for channel in guild.text_channels:
                result.append((guild.name, channel.name, str(channel.id)))
        return result

    def fetch_channels(self, bot_name):
        return self.run_coro(self._fetch_channels(bot_name))

    async def _fetch_channel_snapshot(self, bot_name, channel_id, limit=30):
        client = self.clients.get(bot_name)
        if not client or not client.is_ready():
            return {"error": "Bot is not logged in"}
        try:
            channel_obj = client.get_channel(int(channel_id))
            if channel_obj is None:
                channel_obj = await client.fetch_channel(int(channel_id))

            meta = {
                "guild": getattr(getattr(channel_obj, "guild", None), "name", "Direct or unknown"),
                "channel_name": getattr(channel_obj, "name", "unknown"),
                "channel_id": str(getattr(channel_obj, "id", channel_id)),
                "topic": getattr(channel_obj, "topic", "") or "",
                "nsfw": bool(getattr(channel_obj, "nsfw", False)),
                "slowmode_delay": int(getattr(channel_obj, "slowmode_delay", 0) or 0),
                "member_count": int(getattr(getattr(channel_obj, "guild", None), "member_count", 0) or 0),
            }

            messages = []
            async for message in channel_obj.history(limit=limit):
                raw_text = message.content or ""
                if message.attachments:
                    names = ", ".join(att.filename for att in message.attachments)
                    if raw_text:
                        raw_text += f" [attachments: {names}]"
                    else:
                        raw_text = f"[attachments: {names}]"
                if not raw_text:
                    raw_text = "[no text]"
                stamp = message.created_at.strftime("%Y-%m-%d %H:%M:%S") if message.created_at else "unknown-time"
                messages.append(f"{stamp} | {message.author}: {raw_text}")
            messages.reverse()
            return {"meta": meta, "messages": messages}
        except Exception as exc:
            return {"error": str(exc)}

    def fetch_channel_snapshot(self, bot_name, channel_id, limit=30):
        return self.run_coro(self._fetch_channel_snapshot(bot_name, channel_id, limit))

    async def _shutdown(self):
        for bot_name in list(self.clients.keys()):
            await self._close_bot(bot_name)

    def shutdown(self):
        if not self.loop:
            return
        try:
            self.run_coro(self._shutdown()).result(timeout=6)
        except Exception:
            pass
        try:
            self.loop.call_soon_threadsafe(self.loop.stop)
        except Exception:
            pass
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)


class DiscordDesktopApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Discord Bot Studio")
        self.root.geometry("1280x800")
        self.root.minsize(1080, 680)

        ensure_storage()
        self.bots = load_kv_file(BOT_FILE)
        self.channels = load_kv_file(CHANNEL_FILE)
        self.members = load_kv_file(MEMBER_FILE)
        self.settings = load_settings()
        self.theme_name = self.settings.get("theme", "Midnight")
        if self.theme_name not in THEMES:
            self.theme_name = "Midnight"
        self.templates = normalize_templates(self.settings.get("templates", DEFAULT_TEMPLATES[:]))
        self.embed_presets = normalize_embed_presets(self.settings.get("embed_presets", DEFAULT_EMBED_PRESETS[:]))
        self.activity_feed = self.settings.get("activity_feed", [])
        if not isinstance(self.activity_feed, list):
            self.activity_feed = []
        self.activity_feed = [str(item) for item in self.activity_feed[-MAX_ACTIVITY_ITEMS:]]
        self.notes_text = str(self.settings.get("notes_text", ""))
        self.draft_text = str(self.settings.get("draft_text", ""))
        self.favorite_bots = [str(item) for item in self.settings.get("favorite_bots", []) if isinstance(item, str)]
        self.favorite_channels = [str(item) for item in self.settings.get("favorite_channels", []) if isinstance(item, str)]
        self.favorite_bots = [str(item) for item in self.settings.get("favorite_bots", []) if isinstance(item, str)]
        self.favorite_channels = [str(item) for item in self.settings.get("favorite_channels", []) if isinstance(item, str)]
        self.bulk_history = [item for item in self.settings.get("bulk_history", []) if isinstance(item, dict)]
        self.bulk_history = self.bulk_history[-MAX_BULK_HISTORY_ITEMS:]
        self.next_bulk_campaign_id = max((int(item.get("id", 0)) for item in self.bulk_history), default=0) + 1
        self.running_bulk_campaign = None

        self.active_bot = self.settings.get("preferred_bot")
        if self.active_bot not in self.bots:
            self.active_bot = next(iter(self.bots.keys()), None)
        self.active_channel = self.settings.get("active_channel")
        if self.active_channel not in self.channels:
            self.active_channel = next(iter(self.channels.keys()), None)

        self.scheduled_jobs = deserialize_schedule_jobs(self.settings.get("scheduled_jobs", []))
        self.next_schedule_id = max((job["id"] for job in self.scheduled_jobs), default=0) + 1

        self.bot_filter_var = tk.StringVar()
        self.channel_filter_var = tk.StringVar()
        self.template_filter_var = tk.StringVar()
        self.visible_template_indices = []
        self.visible_template_indices = []

        self.event_queue = queue.Queue()
        self.runtime = DiscordRuntime(self.event_queue)

        self.build_styles()
        self.build_ui()
        self.bind_shortcuts()
        self.refresh_all_views()
        self.restore_session()

        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.after(120, self.poll_runtime_events)
        self.root.after(1000, self.process_scheduled_jobs)
        self.root.after(400, self.maybe_run_startup_wizard)

    def build_styles(self):
        theme = THEMES[self.theme_name]
        self.root.configure(bg=theme["app_bg"])
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("App.TFrame", background=theme["app_bg"])
        style.configure("Card.TFrame", background=theme["card_bg"])
        style.configure("Header.TLabel", background=theme["app_bg"], foreground=theme["text_main"], font=("Segoe UI", 18, "bold"))
        style.configure("Meta.TLabel", background=theme["app_bg"], foreground=theme["text_sub"], font=("Segoe UI", 10))
        style.configure("App.TButton", font=("Segoe UI", 10, "bold"), padding=8)

    def build_ui(self):
        main = ttk.Frame(self.root, style="App.TFrame")
        main.pack(fill="both", expand=True, padx=14, pady=14)

        ttk.Label(main, text="Discord Bot Studio", style="Header.TLabel").pack(anchor="w")
        ttk.Label(main, text="Multi-bot desktop control, scheduling, channel browser, and live logs", style="Meta.TLabel").pack(anchor="w", pady=(2, 12))

        toolbar = ttk.Frame(main, style="App.TFrame")
        toolbar.pack(fill="x", pady=(0, 10))

        ttk.Label(toolbar, text="Active Bot:", style="Meta.TLabel").pack(side="left")
        self.active_bot_combo = ttk.Combobox(toolbar, state="readonly", width=18)
        self.active_bot_combo.pack(side="left", padx=(6, 10))
        self.active_bot_combo.bind("<<ComboboxSelected>>", lambda _e: self.on_active_bot_changed())

        ttk.Label(toolbar, text="Active Channel:", style="Meta.TLabel").pack(side="left")
        self.active_channel_combo = ttk.Combobox(toolbar, state="readonly", width=26)
        self.active_channel_combo.pack(side="left", padx=(6, 10))
        self.active_channel_combo.bind("<<ComboboxSelected>>", lambda _e: self.on_active_channel_changed())

        ttk.Button(toolbar, text="Login Bot", style="App.TButton", command=self.login_selected_bot).pack(side="left", padx=4)
        ttk.Button(toolbar, text="Logout Bot", style="App.TButton", command=self.logout_selected_bot).pack(side="left", padx=4)
        ttk.Button(toolbar, text="Login All", style="App.TButton", command=self.login_all_bots).pack(side="left", padx=4)
        ttk.Button(toolbar, text="Logout All", style="App.TButton", command=self.logout_all_bots).pack(side="left", padx=4)
        ttk.Button(toolbar, text="Browse Servers", style="App.TButton", command=self.browse_discord_servers).pack(side="left", padx=4)
        ttk.Button(toolbar, text="Import Channels", style="App.TButton", command=self.import_channels_from_discord).pack(side="left", padx=4)
        ttk.Button(toolbar, text="Theme", style="App.TButton", command=self.choose_theme).pack(side="left", padx=4)
        ttk.Button(toolbar, text="Export Backup", style="App.TButton", command=self.export_backup).pack(side="left", padx=4)
        ttk.Button(toolbar, text="Export Activity", style="App.TButton", command=self.export_activity_log).pack(side="left", padx=4)
        ttk.Button(toolbar, text="Import Backup", style="App.TButton", command=self.import_backup).pack(side="left", padx=4)
        ttk.Button(toolbar, text="Status", style="App.TButton", command=self.show_status_dialog).pack(side="left", padx=4)

        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(main, textvariable=self.status_var, style="Meta.TLabel").pack(anchor="w", pady=(0, 8))

        content = ttk.PanedWindow(main, orient="horizontal")
        content.pack(fill="both", expand=True)

        left = ttk.Frame(content, style="Card.TFrame")
        right = ttk.Frame(content, style="Card.TFrame")
        content.add(left, weight=1)
        content.add(right, weight=2)

        self.build_left_panel(left)
        self.build_right_panel(right)

    def build_left_panel(self, parent):
        notebook = ttk.Notebook(parent)
        notebook.pack(fill="both", expand=True, padx=10, pady=10)

        bots_tab = ttk.Frame(notebook, style="Card.TFrame")
        channels_tab = ttk.Frame(notebook, style="Card.TFrame")
        members_tab = ttk.Frame(notebook, style="Card.TFrame")
        sched_tab = ttk.Frame(notebook, style="Card.TFrame")
        templates_tab = ttk.Frame(notebook, style="Card.TFrame")
        embeds_tab = ttk.Frame(notebook, style="Card.TFrame")
        notes_tab = ttk.Frame(notebook, style="Card.TFrame")
        notebook.add(bots_tab, text="Bots")
        notebook.add(channels_tab, text="Channels")
        notebook.add(members_tab, text="Members")
        notebook.add(sched_tab, text="Scheduler")
        notebook.add(templates_tab, text="Templates")
        notebook.add(embeds_tab, text="Embeds")
        notebook.add(notes_tab, text="Notes")

        theme = THEMES[self.theme_name]

        bot_filter_row = ttk.Frame(bots_tab, style="Card.TFrame")
        bot_filter_row.pack(fill="x", padx=10, pady=(10, 0))
        ttk.Label(bot_filter_row, text="Filter:", style="Meta.TLabel").pack(side="left")
        ttk.Entry(bot_filter_row, textvariable=self.bot_filter_var).pack(side="left", fill="x", expand=True, padx=(8, 0))
        self.bot_filter_var.trace_add("write", lambda *_args: self.refresh_all_views())

        self.bot_list = tk.Listbox(bots_tab, bg=theme["list_bg"], fg=theme["text_main"], selectbackground=theme["select_bg"], font=("Consolas", 10))
        self.bot_list.pack(fill="both", expand=True, padx=10, pady=10)
        bot_actions = ttk.Frame(bots_tab, style="Card.TFrame")
        bot_actions.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(bot_actions, text="Add", style="App.TButton", command=self.add_bot).pack(side="left", padx=(0, 6))
        ttk.Button(bot_actions, text="Edit", style="App.TButton", command=self.edit_bot).pack(side="left", padx=(0, 6))
        ttk.Button(bot_actions, text="Delete", style="App.TButton", command=self.delete_bot).pack(side="left", padx=(0, 6))
        ttk.Button(bot_actions, text="Favorite", style="App.TButton", command=self.toggle_favorite_bot).pack(side="left", padx=(0, 6))
        ttk.Button(bot_actions, text="Set Active", style="App.TButton", command=self.select_bot_as_active).pack(side="left", padx=(0, 6))
        ttk.Button(bot_actions, text="Login Selected", style="App.TButton", command=self.login_from_list).pack(side="left")

        channel_filter_row = ttk.Frame(channels_tab, style="Card.TFrame")
        channel_filter_row.pack(fill="x", padx=10, pady=(10, 0))
        ttk.Label(channel_filter_row, text="Filter:", style="Meta.TLabel").pack(side="left")
        ttk.Entry(channel_filter_row, textvariable=self.channel_filter_var).pack(side="left", fill="x", expand=True, padx=(8, 0))
        self.channel_filter_var.trace_add("write", lambda *_args: self.refresh_all_views())

        self.channel_list = tk.Listbox(channels_tab, bg=theme["list_bg"], fg=theme["text_main"], selectbackground=theme["select_bg"], font=("Consolas", 10))
        self.channel_list.pack(fill="both", expand=True, padx=10, pady=10)
        channel_actions = ttk.Frame(channels_tab, style="Card.TFrame")
        channel_actions.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(channel_actions, text="Add", style="App.TButton", command=self.add_channel).pack(side="left", padx=(0, 6))
        ttk.Button(channel_actions, text="Edit", style="App.TButton", command=self.edit_channel).pack(side="left", padx=(0, 6))
        ttk.Button(channel_actions, text="Delete", style="App.TButton", command=self.delete_channel).pack(side="left", padx=(0, 6))
        ttk.Button(channel_actions, text="Favorite", style="App.TButton", command=self.toggle_favorite_channel).pack(side="left", padx=(0, 6))
        ttk.Button(channel_actions, text="Set Active", style="App.TButton", command=self.select_channel_as_active).pack(side="left")

        self.member_list = tk.Listbox(members_tab, bg=theme["list_bg"], fg=theme["text_main"], selectbackground=theme["select_bg"], font=("Consolas", 10))
        self.member_list.pack(fill="both", expand=True, padx=10, pady=10)
        member_actions = ttk.Frame(members_tab, style="Card.TFrame")
        member_actions.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(member_actions, text="Add", style="App.TButton", command=self.add_member).pack(side="left", padx=(0, 6))
        ttk.Button(member_actions, text="Edit", style="App.TButton", command=self.edit_member).pack(side="left", padx=(0, 6))
        ttk.Button(member_actions, text="Delete", style="App.TButton", command=self.delete_member).pack(side="left")

        sched_form = ttk.Frame(sched_tab, style="Card.TFrame")
        sched_form.pack(fill="x", padx=10, pady=(10, 6))
        ttk.Label(sched_form, text="Message:", style="Meta.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(sched_form, text="Delay (sec):", style="Meta.TLabel").grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Label(sched_form, text="Repeat every (sec):", style="Meta.TLabel").grid(row=2, column=0, sticky="w", pady=(6, 0))
        ttk.Label(sched_form, text="Total sends:", style="Meta.TLabel").grid(row=3, column=0, sticky="w", pady=(6, 0))
        self.schedule_text_var = tk.StringVar()
        self.schedule_delay_var = tk.StringVar(value="10")
        self.schedule_interval_var = tk.StringVar(value="0")
        self.schedule_repeat_var = tk.StringVar(value="1")
        ttk.Entry(sched_form, textvariable=self.schedule_text_var, width=38).grid(row=0, column=1, sticky="ew", padx=(8, 0))
        ttk.Entry(sched_form, textvariable=self.schedule_delay_var, width=10).grid(row=1, column=1, sticky="w", padx=(8, 0), pady=(6, 0))
        ttk.Entry(sched_form, textvariable=self.schedule_interval_var, width=10).grid(row=2, column=1, sticky="w", padx=(8, 0), pady=(6, 0))
        ttk.Entry(sched_form, textvariable=self.schedule_repeat_var, width=10).grid(row=3, column=1, sticky="w", padx=(8, 0), pady=(6, 0))
        sched_form.columnconfigure(1, weight=1)

        sched_actions = ttk.Frame(sched_tab, style="Card.TFrame")
        sched_actions.pack(fill="x", padx=10, pady=(0, 6))
        ttk.Button(sched_actions, text="Schedule", style="App.TButton", command=self.schedule_message).pack(side="left", padx=(0, 6))
        ttk.Button(sched_actions, text="Edit Selected", style="App.TButton", command=self.edit_selected_schedule).pack(side="left", padx=(0, 6))
        ttk.Button(sched_actions, text="Duplicate", style="App.TButton", command=self.duplicate_selected_schedule).pack(side="left", padx=(0, 6))
        ttk.Button(sched_actions, text="Pause/Resume", style="App.TButton", command=self.toggle_selected_schedule_pause).pack(side="left", padx=(0, 6))
        ttk.Button(sched_actions, text="Job Report", style="App.TButton", command=self.show_selected_schedule_report).pack(side="left", padx=(0, 6))
        ttk.Button(sched_actions, text="Export Jobs", style="App.TButton", command=self.export_schedule_jobs).pack(side="left", padx=(0, 6))
        ttk.Button(sched_actions, text="Cancel Selected", style="App.TButton", command=self.cancel_selected_schedule).pack(side="left")

        self.schedule_list = tk.Listbox(sched_tab, bg=theme["list_bg"], fg=theme["text_main"], selectbackground=theme["select_bg"], font=("Consolas", 10))
        self.schedule_list.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        template_filter_row = ttk.Frame(templates_tab, style="Card.TFrame")
        template_filter_row.pack(fill="x", padx=10, pady=(10, 0))
        ttk.Label(template_filter_row, text="Filter:", style="Meta.TLabel").pack(side="left")
        ttk.Entry(template_filter_row, textvariable=self.template_filter_var).pack(side="left", fill="x", expand=True, padx=(8, 0))
        self.template_filter_var.trace_add("write", lambda *_args: self.refresh_all_views())

        self.template_list = tk.Listbox(templates_tab, bg=theme["list_bg"], fg=theme["text_main"], selectbackground=theme["select_bg"], font=("Consolas", 10))
        self.template_list.pack(fill="both", expand=True, padx=10, pady=10)
        template_actions = ttk.Frame(templates_tab, style="Card.TFrame")
        template_actions.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(template_actions, text="Add", style="App.TButton", command=self.add_template).pack(side="left", padx=(0, 6))
        ttk.Button(template_actions, text="Edit", style="App.TButton", command=self.edit_template).pack(side="left", padx=(0, 6))
        ttk.Button(template_actions, text="Delete", style="App.TButton", command=self.delete_template).pack(side="left", padx=(0, 6))
        ttk.Button(template_actions, text="Use In Chat", style="App.TButton", command=self.use_template).pack(side="left")

        self.embed_preset_list = tk.Listbox(embeds_tab, bg=theme["list_bg"], fg=theme["text_main"], selectbackground=theme["select_bg"], font=("Consolas", 10))
        self.embed_preset_list.pack(fill="both", expand=True, padx=10, pady=10)
        self.embed_preset_list.bind("<Double-Button-1>", lambda _e: self.send_selected_embed_preset())
        embed_actions = ttk.Frame(embeds_tab, style="Card.TFrame")
        embed_actions.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(embed_actions, text="Add", style="App.TButton", command=self.add_embed_preset).pack(side="left", padx=(0, 6))
        ttk.Button(embed_actions, text="Edit", style="App.TButton", command=self.edit_embed_preset).pack(side="left", padx=(0, 6))
        ttk.Button(embed_actions, text="Duplicate", style="App.TButton", command=self.duplicate_embed_preset).pack(side="left", padx=(0, 6))
        ttk.Button(embed_actions, text="Delete", style="App.TButton", command=self.delete_embed_preset).pack(side="left", padx=(0, 6))
        ttk.Button(embed_actions, text="Send Preset", style="App.TButton", command=self.send_selected_embed_preset).pack(side="left")

        ttk.Label(notes_tab, text="Workspace Notes", style="Meta.TLabel").pack(anchor="w", padx=10, pady=(10, 6))
        self.notes_box = tk.Text(notes_tab, bg=theme["list_bg"], fg=theme["text_main"], insertbackground=theme["text_main"], wrap="word", font=("Consolas", 10))
        self.notes_box.pack(fill="both", expand=True, padx=10, pady=(0, 8))
        self.notes_box.insert("1.0", self.notes_text)
        notes_actions = ttk.Frame(notes_tab, style="Card.TFrame")
        notes_actions.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(notes_actions, text="Save Notes", style="App.TButton", command=self.save_notes).pack(side="left", padx=(0, 6))
        ttk.Button(notes_actions, text="Clear Notes", style="App.TButton", command=self.clear_notes).pack(side="left")

    def build_right_panel(self, parent):
        notebook = ttk.Notebook(parent)
        notebook.pack(fill="both", expand=True, padx=10, pady=10)

        chat_tab = ttk.Frame(notebook, style="Card.TFrame")
        dashboard_tab = ttk.Frame(notebook, style="Card.TFrame")
        notebook.add(chat_tab, text="Chat")
        notebook.add(dashboard_tab, text="Dashboard")

        top = ttk.Frame(chat_tab, style="Card.TFrame")
        top.pack(fill="x", pady=(0, 6))
        ttk.Label(top, text="Live Chat", style="Meta.TLabel").pack(side="left")
        ttk.Button(top, text="Bulk Campaign", style="App.TButton", command=self.show_bulk_campaign_dialog).pack(side="right", padx=(0, 6))
        ttk.Button(top, text="Channel Inspector", style="App.TButton", command=self.show_channel_inspector_dialog).pack(side="right", padx=(0, 6))
        ttk.Button(top, text="Search Log", style="App.TButton", command=self.search_log_dialog).pack(side="right", padx=(0, 6))
        ttk.Button(top, text="Export Log", style="App.TButton", command=self.export_current_log).pack(side="right", padx=(0, 6))
        ttk.Button(top, text="Show Log", style="App.TButton", command=self.show_log_dialog).pack(side="right")

        theme = THEMES[self.theme_name]
        self.chat_box = tk.Text(chat_tab, bg=theme["list_bg"], fg=theme["text_main"], insertbackground=theme["text_main"], wrap="word", font=("Consolas", 10))
        self.chat_box.pack(fill="both", expand=True)
        self.chat_box.config(state="disabled")

        bottom = ttk.Frame(chat_tab, style="Card.TFrame")
        bottom.pack(fill="x", pady=(8, 0))
        self.message_var = tk.StringVar(value=self.draft_text)
        self.message_var.trace_add("write", lambda *_args: self.on_draft_changed())
        self.message_entry = ttk.Entry(bottom, textvariable=self.message_var)
        self.message_entry.pack(side="left", fill="x", expand=True, padx=(0, 8))
        self.message_entry.bind("<Return>", lambda _e: self.send_message())
        ttk.Button(bottom, text="Send", style="App.TButton", command=self.send_message).pack(side="left", padx=(0, 6))
        ttk.Button(bottom, text="Send Embed", style="App.TButton", command=self.send_embed_dialog).pack(side="left", padx=(0, 6))
        ttk.Button(bottom, text="Send File", style="App.TButton", command=self.send_file_dialog).pack(side="left", padx=(0, 6))
        ttk.Button(bottom, text="Clear Chat", style="App.TButton", command=self.clear_chat).pack(side="left")

        stats = ttk.Frame(dashboard_tab, style="Card.TFrame")
        stats.pack(fill="x", pady=(0, 8))
        self.dashboard_stats_var = tk.StringVar(value="Loading dashboard...")
        ttk.Label(stats, textvariable=self.dashboard_stats_var, style="Meta.TLabel", justify="left").pack(anchor="w")

        actions = ttk.Frame(dashboard_tab, style="Card.TFrame")
        actions.pack(fill="x", pady=(0, 8))
        ttk.Button(actions, text="Run Setup Wizard", style="App.TButton", command=self.force_startup_wizard).pack(side="left", padx=(0, 6))
        ttk.Button(actions, text="Load Recent Log", style="App.TButton", command=self.load_recent_log_to_chat).pack(side="left", padx=(0, 6))
        ttk.Button(actions, text="Use First Template", style="App.TButton", command=self.use_first_matching_template).pack(side="left")

        ttk.Label(dashboard_tab, text="Recent Activity", style="Meta.TLabel").pack(anchor="w")
        self.activity_list = tk.Listbox(dashboard_tab, bg=theme["list_bg"], fg=theme["text_main"], selectbackground=theme["select_bg"], font=("Consolas", 10))
        self.activity_list.pack(fill="both", expand=True, pady=(6, 0))

    def refresh_all_views(self):
        bot_sel = self.bot_list.curselection()
        channel_sel = self.channel_list.curselection()
        member_sel = self.member_list.curselection()
        template_sel = self.template_list.curselection()

        bot_keys = self.get_filtered_bot_keys()
        self.bot_list.delete(0, tk.END)
        for bot_name in bot_keys:
            status = "ON" if self.runtime.is_logged_in(bot_name) else "OFF"
            active = "*" if bot_name == self.active_bot else " "
            favorite = "★" if bot_name in self.favorite_bots else " "
            self.bot_list.insert(tk.END, f"{active}{favorite} [{status}] {bot_name} ({mask_token(self.bots[bot_name])})")
        if bot_sel and bot_sel[0] < self.bot_list.size():
            self.bot_list.selection_set(bot_sel[0])

        channel_keys = self.get_filtered_channel_keys()
        self.channel_list.delete(0, tk.END)
        for channel_name in channel_keys:
            active = "*" if channel_name == self.active_channel else " "
            favorite = "★" if channel_name in self.favorite_channels else " "
            self.channel_list.insert(tk.END, f"{active}{favorite} {channel_name} : {self.channels[channel_name]}")
        if channel_sel and channel_sel[0] < self.channel_list.size():
            self.channel_list.selection_set(channel_sel[0])

        self.member_list.delete(0, tk.END)
        for name, mid in self.members.items():
            self.member_list.insert(tk.END, f"{name} : {mid}")
        if member_sel and member_sel[0] < self.member_list.size():
            self.member_list.selection_set(member_sel[0])

        self.visible_template_indices = self.get_visible_template_indices()
        self.template_list.delete(0, tk.END)
        for index in self.visible_template_indices:
            self.template_list.insert(tk.END, template_label(self.templates[index]))
        if template_sel and template_sel[0] < self.template_list.size():
            self.template_list.selection_set(template_sel[0])

        if hasattr(self, "embed_preset_list"):
            embed_sel = self.embed_preset_list.curselection()
            self.embed_preset_list.delete(0, tk.END)
            for preset in self.embed_presets:
                self.embed_preset_list.insert(tk.END, embed_preset_label(preset))
            if embed_sel and embed_sel[0] < self.embed_preset_list.size():
                self.embed_preset_list.selection_set(embed_sel[0])

        if hasattr(self, "activity_list"):
            self.activity_list.delete(0, tk.END)
            for item in reversed(self.activity_feed[-MAX_ACTIVITY_ITEMS:]):
                self.activity_list.insert(tk.END, item)

        if hasattr(self, "dashboard_stats_var"):
            connected_count = sum(1 for name in self.bots if self.runtime.is_logged_in(name))
            recurring_count = sum(1 for job in self.scheduled_jobs if job.get("remaining_runs", 1) > 1)
            self.dashboard_stats_var.set(
                f"Bots: {len(self.bots)} saved / {connected_count} connected\n"
                f"Channels: {len(self.channels)}\n"
                f"Templates: {len(self.templates)}\n"
                f"Embed presets: {len(self.embed_presets)}\n"
                f"Scheduled: {len(self.scheduled_jobs)} total, {recurring_count} recurring\n"
                f"Bulk campaigns: {len(self.bulk_history)} total\n"
                f"Active bot: {self.active_bot or 'None'}\n"
                f"Active channel: {self.active_channel or 'None'}"
            )

        combo_bot_values = sorted(self.bots.keys(), key=lambda key: (key not in self.favorite_bots, key.lower()))
        self.active_bot_combo["values"] = combo_bot_values
        if self.active_bot in combo_bot_values:
            self.active_bot_combo.set(self.active_bot)
        elif combo_bot_values:
            self.active_bot = combo_bot_values[0]
            self.active_bot_combo.set(self.active_bot)
        else:
            self.active_bot = None
            self.active_bot_combo.set("")

        combo_channel_values = sorted(self.channels.keys(), key=lambda key: (key not in self.favorite_channels, key.lower()))
        self.active_channel_combo["values"] = combo_channel_values
        if self.active_channel in combo_channel_values:
            self.active_channel_combo.set(self.active_channel)
        elif combo_channel_values:
            self.active_channel = combo_channel_values[0]
            self.active_channel_combo.set(self.active_channel)
        else:
            self.active_channel = None
            self.active_channel_combo.set("")

        self.refresh_schedule_list()

    def refresh_schedule_list(self):
        self.schedule_list.delete(0, tk.END)
        now = datetime.utcnow()
        for job in self.scheduled_jobs:
            seconds = max(0, int((job["run_at"] - now).total_seconds()))
            preview = job["text"][:50].replace("\n", " ")
            repeat = job.get("remaining_runs", 1)
            interval = job.get("interval_seconds", 0)
            state = "PAUSED" if job.get("paused", False) else "RUNNING"
            run_count = job.get("run_count", 0)
            fail_count = job.get("fail_count", 0)
            self.schedule_list.insert(
                tk.END,
                f"#{job['id']} [{state}] in {seconds}s x{repeat} every {interval}s ok:{run_count} fail:{fail_count} [{job['bot']} -> {job['channel_name']}] {preview}",
            )

    def save_state(self):
        self.settings["preferred_bot"] = self.active_bot
        self.settings["active_channel"] = self.active_channel
        self.settings["theme"] = self.theme_name
        self.settings["templates"] = self.templates
        self.settings["embed_presets"] = self.embed_presets
        self.settings["favorite_bots"] = self.favorite_bots
        self.settings["favorite_channels"] = self.favorite_channels
        self.settings["scheduled_jobs"] = [serialize_schedule_job(job) for job in self.scheduled_jobs]
        self.settings["activity_feed"] = self.activity_feed[-MAX_ACTIVITY_ITEMS:]
        self.settings["bulk_history"] = self.bulk_history[-MAX_BULK_HISTORY_ITEMS:]
        self.settings["notes_text"] = self.notes_text
        self.settings["draft_text"] = self.draft_text
        self.settings["last_updated"] = datetime.utcnow().isoformat() + "Z"
        save_settings(self.settings)

    def add_activity(self, text):
        stamp = datetime.utcnow().strftime("%H:%M:%S")
        self.activity_feed.append(f"{stamp} {text}")
        self.activity_feed = self.activity_feed[-MAX_ACTIVITY_ITEMS:]
        self.save_state()

    def current_template_matches(self, template):
        bot_scope = template.get("bot", "")
        channel_scope = template.get("channel", "")
        if bot_scope and bot_scope != self.active_bot:
            return False
        if channel_scope and channel_scope != self.active_channel:
            return False
        return True

    def get_filtered_bot_keys(self):
        filter_text = self.bot_filter_var.get().strip().lower()
        keys = [key for key in self.bots.keys() if not filter_text or filter_text in key.lower()]
        return sorted(keys, key=lambda key: (key not in self.favorite_bots, key.lower()))

    def get_filtered_channel_keys(self):
        filter_text = self.channel_filter_var.get().strip().lower()
        keys = [
            key
            for key in self.channels.keys()
            if not filter_text or filter_text in key.lower() or filter_text in str(self.channels[key]).lower()
        ]
        return sorted(keys, key=lambda key: (key not in self.favorite_channels, key.lower()))

    def get_visible_template_indices(self):
        filter_text = self.template_filter_var.get().strip().lower()
        visible = []
        for index, item in enumerate(self.templates):
            label = template_label(item)
            if filter_text and filter_text not in label.lower():
                continue
            visible.append(index)
        return visible

    def on_draft_changed(self):
        self.draft_text = self.message_var.get()
        self.save_state()

    def restore_session(self):
        for bot_name in self.settings.get("auto_login_bots", []):
            if bot_name in self.bots:
                self.runtime.login(bot_name, self.bots[bot_name])
        if not self.settings.get("auto_login_bots") and self.active_bot and self.active_bot in self.bots:
            self.runtime.login(self.active_bot, self.bots[self.active_bot])
        if self.active_channel:
            self.load_recent_log_to_chat()
            self.append_chat_line(f"[System] Restored channel: {self.active_channel}")

    def get_selected_key_from_list(self, listbox, mapping):
        selection = listbox.curselection()
        if not selection:
            return None
        idx = selection[0]
        if mapping is self.bots:
            keys = self.get_filtered_bot_keys()
        elif mapping is self.channels:
            keys = self.get_filtered_channel_keys()
        elif mapping is self.members:
            keys = list(mapping.keys())
        else:
            keys = list(mapping.keys())
        return keys[idx] if 0 <= idx < len(keys) else None

    def save_notes(self):
        self.notes_text = self.notes_box.get("1.0", "end-1c")
        self.save_state()
        self.set_status("Notes saved")
        self.add_activity("Saved notes")

    def clear_notes(self):
        if not messagebox.askyesno("Notes", "Clear all notes?"):
            return
        self.notes_box.delete("1.0", "end")
        self.notes_text = ""
        self.save_state()
        self.set_status("Notes cleared")
        self.add_activity("Cleared notes")

    def on_active_bot_changed(self):
        value = self.active_bot_combo.get().strip()
        if value:
            self.active_bot = value
            self.save_state()
            self.refresh_all_views()

    def on_active_channel_changed(self):
        value = self.active_channel_combo.get().strip()
        if value:
            self.active_channel = value
            self.save_state()
            self.refresh_all_views()
            self.load_recent_log_to_chat()

    def bind_shortcuts(self):
        self.root.bind("<Control-Return>", lambda _e: self.send_message())
        self.root.bind("<Control-l>", lambda _e: self.load_recent_log_to_chat())
        self.root.bind("<Control-f>", lambda _e: self.search_log_dialog())

    def add_bot(self):
        name = simpledialog.askstring("Add Bot", "Bot name:", parent=self.root)
        if not name:
            return
        token = simpledialog.askstring("Add Bot", "Bot token:", parent=self.root)
        if not token:
            return
        self.bots[name.strip()] = token.strip()
        save_kv_file(BOT_FILE, self.bots)
        if not self.active_bot:
            self.active_bot = name.strip()
        self.save_state()
        self.refresh_all_views()
        self.set_status("Bot added")
        self.add_activity(f"Added bot {name.strip()}")

    def edit_bot(self):
        bot_name = self.get_selected_key_from_list(self.bot_list, self.bots)
        if not bot_name:
            messagebox.showinfo("Edit Bot", "Select a bot first")
            return
        new_name = simpledialog.askstring("Edit Bot", "Bot name:", initialvalue=bot_name, parent=self.root)
        if not new_name:
            return
        new_token = simpledialog.askstring("Edit Bot", "Bot token:", initialvalue=self.bots[bot_name], parent=self.root)
        if not new_token:
            return
        new_name = new_name.strip()
        new_token = new_token.strip()
        if new_name != bot_name and new_name in self.bots:
            messagebox.showerror("Edit Bot", "A bot with that name already exists")
            return
        self.bots.pop(bot_name, None)
        self.bots[new_name] = new_token
        if self.active_bot == bot_name:
            self.active_bot = new_name
        save_kv_file(BOT_FILE, self.bots)
        self.save_state()
        self.refresh_all_views()
        self.add_activity(f"Edited bot {bot_name} -> {new_name}")

    def toggle_favorite_bot(self):
        bot_name = self.get_selected_key_from_list(self.bot_list, self.bots)
        if not bot_name:
            messagebox.showinfo("Favorite Bot", "Select a bot first")
            return
        if bot_name in self.favorite_bots:
            self.favorite_bots = [item for item in self.favorite_bots if item != bot_name]
            self.add_activity(f"Removed favorite bot {bot_name}")
        else:
            self.favorite_bots.append(bot_name)
            self.add_activity(f"Added favorite bot {bot_name}")
        self.save_state()
        self.refresh_all_views()

    def delete_bot(self):
        bot_name = self.get_selected_key_from_list(self.bot_list, self.bots)
        if not bot_name:
            messagebox.showinfo("Delete Bot", "Select a bot first")
            return
        if not messagebox.askyesno("Delete Bot", f"Delete {bot_name}?"):
            return
        if self.runtime.is_logged_in(bot_name):
            self.runtime.logout(bot_name)
        self.bots.pop(bot_name, None)
        save_kv_file(BOT_FILE, self.bots)
        if self.active_bot == bot_name:
            self.active_bot = next(iter(self.bots.keys()), None)
        self.save_state()
        self.refresh_all_views()
        self.add_activity(f"Deleted bot {bot_name}")

    def select_bot_as_active(self):
        bot_name = self.get_selected_key_from_list(self.bot_list, self.bots)
        if not bot_name:
            messagebox.showinfo("Set Active", "Select a bot first")
            return
        self.active_bot = bot_name
        self.save_state()
        self.refresh_all_views()
        self.add_activity(f"Set active bot to {bot_name}")

    def login_selected_bot(self):
        if not self.active_bot or self.active_bot not in self.bots:
            messagebox.showinfo("Login", "Select an active bot first")
            return
        self.runtime.login(self.active_bot, self.bots[self.active_bot])
        self.settings["auto_login_bots"] = list({*self.settings.get("auto_login_bots", []), self.active_bot})
        self.save_state()
        self.set_status(f"Login requested for {self.active_bot}")
        self.add_activity(f"Requested login for {self.active_bot}")

    def login_all_bots(self):
        if not self.bots:
            messagebox.showinfo("Login All", "No bots saved")
            return
        for bot_name, token in self.bots.items():
            self.runtime.login(bot_name, token)
        self.settings["auto_login_bots"] = list(self.bots.keys())
        self.save_state()
        self.set_status("Login requested for all bots")
        self.add_activity("Requested login for all bots")

    def login_from_list(self):
        bot_name = self.get_selected_key_from_list(self.bot_list, self.bots)
        if not bot_name:
            messagebox.showinfo("Login", "Select a bot first")
            return
        self.active_bot = bot_name
        self.login_selected_bot()

    def logout_selected_bot(self):
        if not self.active_bot:
            messagebox.showinfo("Logout", "No active bot selected")
            return
        self.runtime.logout(self.active_bot)
        self.settings["auto_login_bots"] = [b for b in self.settings.get("auto_login_bots", []) if b != self.active_bot]
        self.save_state()
        self.set_status(f"Logout requested for {self.active_bot}")
        self.add_activity(f"Requested logout for {self.active_bot}")

    def logout_all_bots(self):
        connected = [bot_name for bot_name in self.bots if self.runtime.is_logged_in(bot_name)]
        if not connected:
            messagebox.showinfo("Logout All", "No bots are currently logged in")
            return
        for bot_name in connected:
            self.runtime.logout(bot_name)
        self.settings["auto_login_bots"] = []
        self.save_state()
        self.set_status("Logout requested for all bots")
        self.add_activity("Requested logout for all bots")

    def add_channel(self):
        name = simpledialog.askstring("Add Channel", "Channel name:", parent=self.root)
        if not name:
            return
        channel_id = simpledialog.askstring("Add Channel", "Discord Channel ID:", parent=self.root)
        if not channel_id or not channel_id.isdigit():
            messagebox.showerror("Add Channel", "Channel ID must be numeric")
            return
        self.channels[name.strip()] = channel_id.strip()
        save_kv_file(CHANNEL_FILE, self.channels)
        if not self.active_channel:
            self.active_channel = name.strip()
        self.save_state()
        self.refresh_all_views()
        if self.active_channel == name.strip():
            self.load_recent_log_to_chat()
        self.add_activity(f"Added channel {name.strip()}")

    def edit_channel(self):
        channel_name = self.get_selected_key_from_list(self.channel_list, self.channels)
        if not channel_name:
            messagebox.showinfo("Edit Channel", "Select a channel first")
            return
        new_name = simpledialog.askstring("Edit Channel", "Channel name:", initialvalue=channel_name, parent=self.root)
        if not new_name:
            return
        new_id = simpledialog.askstring("Edit Channel", "Discord Channel ID:", initialvalue=self.channels[channel_name], parent=self.root)
        if not new_id or not new_id.isdigit():
            messagebox.showerror("Edit Channel", "Channel ID must be numeric")
            return
        new_name = new_name.strip()
        if new_name != channel_name and new_name in self.channels:
            messagebox.showerror("Edit Channel", "A channel with that name already exists")
            return
        self.channels.pop(channel_name, None)
        self.channels[new_name] = new_id.strip()
        if self.active_channel == channel_name:
            self.active_channel = new_name
        save_kv_file(CHANNEL_FILE, self.channels)
        self.save_state()
        self.refresh_all_views()
        self.load_recent_log_to_chat()
        self.add_activity(f"Edited channel {channel_name} -> {new_name}")

    def toggle_favorite_channel(self):
        channel_name = self.get_selected_key_from_list(self.channel_list, self.channels)
        if not channel_name:
            messagebox.showinfo("Favorite Channel", "Select a channel first")
            return
        if channel_name in self.favorite_channels:
            self.favorite_channels = [item for item in self.favorite_channels if item != channel_name]
            self.add_activity(f"Removed favorite channel {channel_name}")
        else:
            self.favorite_channels.append(channel_name)
            self.add_activity(f"Added favorite channel {channel_name}")
        self.save_state()
        self.refresh_all_views()

    def delete_channel(self):
        channel_name = self.get_selected_key_from_list(self.channel_list, self.channels)
        if not channel_name:
            messagebox.showinfo("Delete Channel", "Select a channel first")
            return
        if not messagebox.askyesno("Delete Channel", f"Delete {channel_name}?"):
            return
        self.channels.pop(channel_name, None)
        save_kv_file(CHANNEL_FILE, self.channels)
        if self.active_channel == channel_name:
            self.active_channel = next(iter(self.channels.keys()), None)
        self.save_state()
        self.refresh_all_views()
        self.add_activity(f"Deleted channel {channel_name}")

    def select_channel_as_active(self):
        channel_name = self.get_selected_key_from_list(self.channel_list, self.channels)
        if not channel_name:
            messagebox.showinfo("Set Active Channel", "Select a channel first")
            return
        self.active_channel = channel_name
        self.save_state()
        self.refresh_all_views()
        self.load_recent_log_to_chat()
        self.add_activity(f"Set active channel to {channel_name}")

    def add_member(self):
        name = simpledialog.askstring("Add Member Alias", "Alias name:", parent=self.root)
        if not name:
            return
        member_id = simpledialog.askstring("Add Member Alias", "Discord User ID:", parent=self.root)
        if not member_id or not member_id.isdigit():
            messagebox.showerror("Add Member Alias", "Discord ID must be numeric")
            return
        self.members[name.strip()] = member_id.strip()
        save_kv_file(MEMBER_FILE, self.members)
        self.refresh_all_views()
        self.add_activity(f"Added member alias {name.strip()}")

    def edit_member(self):
        member_name = self.get_selected_key_from_list(self.member_list, self.members)
        if not member_name:
            messagebox.showinfo("Edit Member", "Select a member first")
            return
        new_name = simpledialog.askstring("Edit Member Alias", "Alias name:", initialvalue=member_name, parent=self.root)
        if not new_name:
            return
        new_id = simpledialog.askstring("Edit Member Alias", "Discord User ID:", initialvalue=self.members[member_name], parent=self.root)
        if not new_id or not new_id.isdigit():
            messagebox.showerror("Edit Member Alias", "Discord ID must be numeric")
            return
        new_name = new_name.strip()
        if new_name != member_name and new_name in self.members:
            messagebox.showerror("Edit Member Alias", "That alias already exists")
            return
        self.members.pop(member_name, None)
        self.members[new_name] = new_id.strip()
        save_kv_file(MEMBER_FILE, self.members)
        self.refresh_all_views()
        self.add_activity(f"Edited member alias {member_name} -> {new_name}")

    def delete_member(self):
        member_name = self.get_selected_key_from_list(self.member_list, self.members)
        if not member_name:
            messagebox.showinfo("Delete Member", "Select a member first")
            return
        if not messagebox.askyesno("Delete Member", f"Delete alias {member_name}?"):
            return
        self.members.pop(member_name, None)
        save_kv_file(MEMBER_FILE, self.members)
        self.refresh_all_views()
        self.add_activity(f"Deleted member alias {member_name}")

    def add_template(self):
        value = simpledialog.askstring("Add Template", "Template text:", parent=self.root)
        if not value:
            return
        scope = messagebox.askyesnocancel(
            "Template Scope",
            "Scope this template to the active bot/channel?\nYes = current bot and channel\nNo = global template",
            parent=self.root,
        )
        if scope is None:
            return
        template = {
            "text": value.strip(),
            "bot": self.active_bot if scope else "",
            "channel": self.active_channel if scope else "",
        }
        self.templates.append(template)
        self.save_state()
        self.refresh_all_views()
        self.add_activity("Added template")

    def edit_template(self):
        sel = self.template_list.curselection()
        if not sel:
            messagebox.showinfo("Templates", "Select a template first")
            return
        visible_index = sel[0]
        if not (0 <= visible_index < len(self.visible_template_indices)):
            return
        idx = self.visible_template_indices[visible_index]
        template = self.templates[idx]
        new_text = simpledialog.askstring("Edit Template", "Template text:", initialvalue=template.get("text", ""), parent=self.root)
        if not new_text:
            return
        use_scope = messagebox.askyesnocancel(
            "Template Scope",
            "Keep this template scoped to the current bot/channel?\nYes = current bot and channel\nNo = global template",
            parent=self.root,
        )
        if use_scope is None:
            return
        template["text"] = new_text.strip()
        template["bot"] = self.active_bot if use_scope else ""
        template["channel"] = self.active_channel if use_scope else ""
        self.save_state()
        self.refresh_all_views()
        self.add_activity("Edited template")

    def delete_template(self):
        sel = self.template_list.curselection()
        if not sel:
            messagebox.showinfo("Templates", "Select a template first")
            return
        visible_index = sel[0]
        if not (0 <= visible_index < len(self.visible_template_indices)):
            return
        idx = self.visible_template_indices[visible_index]
        if 0 <= idx < len(self.templates):
            self.templates.pop(idx)
            self.save_state()
            self.refresh_all_views()
            self.add_activity("Deleted template")

    def use_template(self):
        sel = self.template_list.curselection()
        if not sel:
            messagebox.showinfo("Templates", "Select a template first")
            return
        visible_index = sel[0]
        if not (0 <= visible_index < len(self.visible_template_indices)):
            return
        idx = self.visible_template_indices[visible_index]
        if 0 <= idx < len(self.templates):
            self.message_var.set(self.templates[idx]["text"])
            self.message_entry.focus_set()
            self.add_activity("Loaded template into chat")

    def use_first_matching_template(self):
        for template in self.templates:
            if self.current_template_matches(template):
                self.message_var.set(template["text"])
                self.message_entry.focus_set()
                self.add_activity("Loaded first matching template")
                return
        messagebox.showinfo("Templates", "No template matches the current bot/channel")

    def get_selected_embed_preset(self):
        sel = self.embed_preset_list.curselection()
        if not sel:
            return None, None
        idx = sel[0]
        if not (0 <= idx < len(self.embed_presets)):
            return None, None
        return idx, self.embed_presets[idx]

    def open_embed_editor(self, dialog_title, submit_text, initial=None):
        initial = initial or {}
        result = {}

        dialog = tk.Toplevel(self.root)
        dialog.title(dialog_title)
        dialog.geometry("560x470")
        dialog.transient(self.root)
        dialog.grab_set()

        body = ttk.Frame(dialog, style="Card.TFrame")
        body.pack(fill="both", expand=True, padx=12, pady=12)

        name_var = tk.StringVar(value=initial.get("name", ""))
        title_var = tk.StringVar(value=initial.get("title", ""))
        color_var = tk.StringVar(value=initial.get("color", "#4A90E2"))
        footer_var = tk.StringVar(value=initial.get("footer", ""))

        ttk.Label(body, text="Preset Name", style="Meta.TLabel").pack(anchor="w")
        name_entry = ttk.Entry(body, textvariable=name_var)
        name_entry.pack(fill="x", pady=(0, 8))

        ttk.Label(body, text="Embed Title", style="Meta.TLabel").pack(anchor="w")
        ttk.Entry(body, textvariable=title_var).pack(fill="x", pady=(0, 8))

        top_row = ttk.Frame(body, style="Card.TFrame")
        top_row.pack(fill="x", pady=(0, 8))
        ttk.Label(top_row, text="Color Hex", style="Meta.TLabel").pack(side="left")
        ttk.Entry(top_row, textvariable=color_var, width=14).pack(side="left", padx=(8, 14))
        ttk.Label(top_row, text="Footer", style="Meta.TLabel").pack(side="left")
        ttk.Entry(top_row, textvariable=footer_var).pack(side="left", fill="x", expand=True, padx=(8, 0))

        ttk.Label(body, text="Description", style="Meta.TLabel").pack(anchor="w")
        description_box = tk.Text(body, height=12, wrap="word")
        description_box.pack(fill="both", expand=True, pady=(0, 10))
        description_box.insert("1.0", initial.get("description", ""))

        preview_var = tk.StringVar(value="Preview updates as you type")
        ttk.Label(body, textvariable=preview_var, style="Meta.TLabel", justify="left").pack(anchor="w", pady=(0, 10))

        def refresh_preview(*_args):
            name_text = name_var.get().strip() or "(unnamed)"
            title_text = title_var.get().strip() or "(no title)"
            desc_text = description_box.get("1.0", "end").strip()[:160] or "(no description)"
            preview_var.set(f"{name_text}\nTitle: {title_text}\n{desc_text}")

        def submit():
            name_text = name_var.get().strip()
            if not name_text:
                messagebox.showwarning(dialog_title, "Preset name is required", parent=dialog)
                return
            color_text = color_var.get().strip() or "#4A90E2"
            try:
                int(color_text.replace("#", ""), 16)
            except ValueError:
                messagebox.showwarning(dialog_title, "Color must be valid hex", parent=dialog)
                return
            result.update(
                {
                    "name": name_text,
                    "title": title_var.get().strip(),
                    "description": description_box.get("1.0", "end").strip(),
                    "color": color_text,
                    "footer": footer_var.get().strip(),
                }
            )
            dialog.destroy()

        button_row = ttk.Frame(body, style="Card.TFrame")
        button_row.pack(fill="x")
        ttk.Button(button_row, text=submit_text, style="App.TButton", command=submit).pack(side="left")
        ttk.Button(button_row, text="Cancel", style="App.TButton", command=dialog.destroy).pack(side="left", padx=8)

        name_var.trace_add("write", refresh_preview)
        title_var.trace_add("write", refresh_preview)
        color_var.trace_add("write", refresh_preview)
        footer_var.trace_add("write", refresh_preview)
        description_box.bind("<KeyRelease>", refresh_preview)
        refresh_preview()
        name_entry.focus_set()
        self.root.wait_window(dialog)
        return result or None

    def add_embed_preset(self):
        preset = self.open_embed_editor("Add Embed Preset", "Save Preset")
        if not preset:
            return
        self.embed_presets.append(preset)
        self.save_state()
        self.refresh_all_views()
        self.add_activity(f"Added embed preset {preset['name']}")

    def edit_embed_preset(self):
        idx, preset = self.get_selected_embed_preset()
        if preset is None:
            messagebox.showinfo("Embed Presets", "Select a preset first")
            return
        updated = self.open_embed_editor("Edit Embed Preset", "Save Changes", preset)
        if not updated:
            return
        self.embed_presets[idx] = updated
        self.save_state()
        self.refresh_all_views()
        self.add_activity(f"Edited embed preset {updated['name']}")

    def duplicate_embed_preset(self):
        _idx, preset = self.get_selected_embed_preset()
        if preset is None:
            messagebox.showinfo("Embed Presets", "Select a preset first")
            return
        draft = dict(preset)
        draft["name"] = f"{preset.get('name', 'Preset')} Copy"
        duplicated = self.open_embed_editor("Duplicate Embed Preset", "Save Copy", draft)
        if not duplicated:
            return
        self.embed_presets.append(duplicated)
        self.save_state()
        self.refresh_all_views()
        self.add_activity(f"Duplicated embed preset {duplicated['name']}")

    def delete_embed_preset(self):
        idx, preset = self.get_selected_embed_preset()
        if preset is None:
            messagebox.showinfo("Embed Presets", "Select a preset first")
            return
        if not messagebox.askyesno("Embed Presets", f"Delete preset {preset['name']}?"):
            return
        self.embed_presets.pop(idx)
        self.save_state()
        self.refresh_all_views()
        self.add_activity(f"Deleted embed preset {preset['name']}")

    def send_selected_embed_preset(self):
        _idx, preset = self.get_selected_embed_preset()
        if preset is None:
            messagebox.showinfo("Embed Presets", "Select a preset first")
            return
        if not self.active_bot or self.active_bot not in self.bots:
            messagebox.showwarning("Embed Presets", "Select an active bot")
            return
        if not self.active_channel or self.active_channel not in self.channels:
            messagebox.showwarning("Embed Presets", "Select an active channel")
            return
        try:
            color_value = int(preset.get("color", "#4A90E2").replace("#", ""), 16)
        except ValueError:
            messagebox.showerror("Embed Presets", "Preset color is invalid")
            return
        self.runtime.send_embed(
            self.active_bot,
            self.channels[self.active_channel],
            self.replace_mentions(preset.get("title", "")),
            self.replace_mentions(preset.get("description", "")),
            color_value,
            preset.get("footer", ""),
        )
        self.append_chat_line(f"[{self.active_bot}] You(embed preset): {preset['name']}")
        write_log_line(self.active_channel, f"You(embed preset): {preset['name']}")
        self.set_status("Embed preset queued")
        self.add_activity(f"Queued embed preset {preset['name']}")

    def browse_discord_servers(self):
        if not self.active_bot:
            messagebox.showinfo("Browse Servers", "Select an active bot first")
            return
        if not self.runtime.is_logged_in(self.active_bot):
            messagebox.showinfo("Browse Servers", "Login the active bot first")
            return
        try:
            entries = self.runtime.fetch_channels(self.active_bot).result(timeout=12)
        except Exception as exc:
            messagebox.showerror("Browse Servers", f"Failed to fetch channels: {exc}")
            return
        if not entries:
            messagebox.showinfo("Browse Servers", "No readable text channels found")
            return

        dialog = tk.Toplevel(self.root)
        dialog.title("Browse Discord Servers")
        dialog.geometry("920x620")

        toolbar = ttk.Frame(dialog, style="Card.TFrame")
        toolbar.pack(fill="x", padx=10, pady=(10, 6))
        ttk.Label(toolbar, text="Filter:", style="Meta.TLabel").pack(side="left")
        filter_var = tk.StringVar()
        ttk.Entry(toolbar, textvariable=filter_var).pack(side="left", fill="x", expand=True, padx=(8, 0))

        tree = ttk.Treeview(dialog)
        tree.pack(fill="both", expand=True, padx=10, pady=10)
        tree.heading("#0", text="Guild / Channel")

        channel_lookup = {}
        selected_channel = {"value": None}
        details_var = tk.StringVar(value="Select a channel to view details")

        def rebuild_tree(*_args):
            tree.delete(*tree.get_children())
            channel_lookup.clear()
            guild_nodes = {}
            filter_text = filter_var.get().strip().lower()
            for guild_name, channel_name, channel_id in entries:
                combined = f"{guild_name} {channel_name} {channel_id}".lower()
                if filter_text and filter_text not in combined:
                    continue
                if guild_name not in guild_nodes:
                    guild_nodes[guild_name] = tree.insert("", "end", text=guild_name, open=True)
                node = tree.insert(guild_nodes[guild_name], "end", text=f"#{channel_name} ({channel_id})")
                channel_lookup[node] = (guild_name, channel_name, channel_id)
            details_var.set("Select a channel to view details")
            selected_channel["value"] = None

        def on_selected(_event=None):
            selected = tree.selection()
            if not selected or selected[0] not in channel_lookup:
                details_var.set("Select a channel to view details")
                selected_channel["value"] = None
                return
            guild_name, channel_name, channel_id = channel_lookup[selected[0]]
            selected_channel["value"] = (guild_name, channel_name, channel_id)
            details_var.set(f"Guild: {guild_name}\nChannel: #{channel_name}\nChannel ID: {channel_id}")

        def import_selected_channel():
            if not selected_channel["value"]:
                messagebox.showinfo("Browse Servers", "Select a channel node")
                return
            guild_name, channel_name, channel_id = selected_channel["value"]
            base = f"{guild_name}-{channel_name}"
            key = base
            suffix = 2
            while key in self.channels:
                key = f"{base}-{suffix}"
                suffix += 1
            self.channels[key] = channel_id
            self.active_channel = key
            save_kv_file(CHANNEL_FILE, self.channels)
            self.save_state()
            self.refresh_all_views()
            self.load_recent_log_to_chat()
            self.add_activity(f"Imported channel from browser {key}")

        def set_active_existing_channel():
            if not selected_channel["value"]:
                messagebox.showinfo("Browse Servers", "Select a channel node")
                return
            _guild_name, channel_name, channel_id = selected_channel["value"]
            for key, value in self.channels.items():
                if str(value) == str(channel_id):
                    self.active_channel = key
                    self.save_state()
                    self.refresh_all_views()
                    self.load_recent_log_to_chat()
                    self.add_activity(f"Set active channel from browser {key}")
                    return
            messagebox.showinfo("Browse Servers", f"Channel #{channel_name} is not imported yet")

        def copy_channel_id():
            if not selected_channel["value"]:
                messagebox.showinfo("Browse Servers", "Select a channel node")
                return
            _guild_name, _channel_name, channel_id = selected_channel["value"]
            self.root.clipboard_clear()
            self.root.clipboard_append(channel_id)
            self.set_status("Channel ID copied")

        tree.bind("<<TreeviewSelect>>", on_selected)
        filter_var.trace_add("write", rebuild_tree)
        rebuild_tree()

        details = ttk.Frame(dialog, style="Card.TFrame")
        details.pack(fill="x", padx=10, pady=(0, 8))
        ttk.Label(details, textvariable=details_var, style="Meta.TLabel", justify="left").pack(anchor="w")

        row = ttk.Frame(dialog)
        row.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(row, text="Import Selected Channel", style="App.TButton", command=import_selected_channel).pack(side="left")
        ttk.Button(row, text="Set Active If Imported", style="App.TButton", command=set_active_existing_channel).pack(side="left", padx=8)
        ttk.Button(row, text="Copy Channel ID", style="App.TButton", command=copy_channel_id).pack(side="left")
        ttk.Button(row, text="Close", style="App.TButton", command=dialog.destroy).pack(side="left", padx=8)

    def replace_mentions(self, message):
        out = message
        for name, member_id in self.members.items():
            out = out.replace("@" + name, f"<@{member_id}>")
        return out

    def send_message(self):
        raw = self.message_var.get().strip()
        if not raw:
            return
        if not self.active_bot or self.active_bot not in self.bots:
            messagebox.showwarning("Send", "Select an active bot")
            return
        if not self.active_channel or self.active_channel not in self.channels:
            messagebox.showwarning("Send", "Select an active channel")
            return

        payload = self.replace_mentions(raw)
        self.runtime.send_message(self.active_bot, payload, self.channels[self.active_channel])
        line = f"You: {payload}"
        self.append_chat_line(f"[{self.active_bot}] {line}")
        write_log_line(self.active_channel, line)
        self.message_var.set("")
        self.set_status("Message queued")
        self.add_activity(f"Queued message for {self.active_bot} -> {self.active_channel}")

    def show_bulk_campaign_dialog(self):
        if not self.active_bot or self.active_bot not in self.bots:
            messagebox.showwarning("Bulk Campaign", "Select an active bot")
            return

        dialog = tk.Toplevel(self.root)
        dialog.title("Bulk Campaign Sender")
        dialog.geometry("930x640")

        top = ttk.Frame(dialog, style="Card.TFrame")
        top.pack(fill="x", padx=10, pady=(10, 8))
        ttk.Label(top, text=f"Bot: {self.active_bot}", style="Meta.TLabel").pack(side="left")
        ttk.Label(top, text="Filter:", style="Meta.TLabel").pack(side="left", padx=(16, 4))
        filter_var = tk.StringVar()
        ttk.Entry(top, textvariable=filter_var).pack(side="left", fill="x", expand=True)

        center = ttk.Frame(dialog, style="Card.TFrame")
        center.pack(fill="both", expand=True, padx=10, pady=(0, 8))

        left = ttk.Frame(center, style="Card.TFrame")
        right = ttk.Frame(center, style="Card.TFrame")
        left.pack(side="left", fill="both", expand=True)
        right.pack(side="left", fill="both", expand=True, padx=(12, 0))

        ttk.Label(left, text="Target Channels (multi-select)", style="Meta.TLabel").pack(anchor="w")
        channel_list = tk.Listbox(left, selectmode=tk.EXTENDED, font=("Consolas", 10))
        channel_list.pack(fill="both", expand=True, pady=(6, 8))
        visible_channels = []

        list_buttons = ttk.Frame(left, style="Card.TFrame")
        list_buttons.pack(fill="x")

        def refill_channels(*_args):
            filter_text = filter_var.get().strip().lower()
            all_keys = sorted(self.channels.keys(), key=lambda key: (key not in self.favorite_channels, key.lower()))
            visible_channels.clear()
            channel_list.delete(0, tk.END)
            for key in all_keys:
                cid = str(self.channels[key])
                combined = f"{key} {cid}".lower()
                if filter_text and filter_text not in combined:
                    continue
                visible_channels.append(key)
                channel_list.insert(tk.END, f"{key} : {cid}")

        def select_all_channels():
            channel_list.selection_set(0, tk.END)

        def clear_channel_selection():
            channel_list.selection_clear(0, tk.END)

        ttk.Button(list_buttons, text="Select All", style="App.TButton", command=select_all_channels).pack(side="left", padx=(0, 6))
        ttk.Button(list_buttons, text="Clear", style="App.TButton", command=clear_channel_selection).pack(side="left")

        ttk.Label(right, text="Campaign Message", style="Meta.TLabel").pack(anchor="w")
        message_box = tk.Text(right, wrap="word", font=("Consolas", 10), height=14)
        message_box.pack(fill="both", expand=True, pady=(6, 8))
        if self.message_var.get().strip():
            message_box.insert("1.0", self.message_var.get().strip())

        options = ttk.Frame(right, style="Card.TFrame")
        options.pack(fill="x")
        ttk.Label(options, text="Delay between channels (sec):", style="Meta.TLabel").pack(side="left")
        delay_var = tk.StringVar(value="0.6")
        ttk.Entry(options, textvariable=delay_var, width=7).pack(side="left", padx=(8, 14))
        apply_mentions_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(options, text="Apply member aliases", variable=apply_mentions_var).pack(side="left")

        progress_var = tk.StringVar(value="Ready")
        ttk.Label(dialog, textvariable=progress_var, style="Meta.TLabel").pack(anchor="w", padx=10, pady=(0, 8))

        def cancel_running_campaign():
            if not self.running_bulk_campaign:
                messagebox.showinfo("Bulk Campaign", "No running campaign")
                return
            self.running_bulk_campaign["status"] = "canceled"
            self.running_bulk_campaign["ended_at"] = datetime.utcnow().isoformat() + "Z"
            self.set_status(f"Campaign #{self.running_bulk_campaign['id']} canceled")
            self.add_activity(f"Canceled bulk campaign #{self.running_bulk_campaign['id']}")
            self.save_state()

        def export_bulk_history():
            path = filedialog.asksaveasfilename(
                parent=dialog,
                title="Export Bulk Campaign History",
                defaultextension=".json",
                filetypes=[("JSON", "*.json")],
                initialfile="discord_bot_studio_bulk_history.json",
            )
            if not path:
                return
            try:
                with open(path, "w", encoding="utf8") as f:
                    json.dump(self.bulk_history[-MAX_BULK_HISTORY_ITEMS:], f, indent=2)
            except OSError as exc:
                messagebox.showerror("Bulk Campaign", f"Failed to export history: {exc}", parent=dialog)
                return
            self.set_status(f"Bulk history exported: {os.path.basename(path)}")
            self.add_activity(f"Exported bulk history {os.path.basename(path)}")

        def start_campaign():
            if self.running_bulk_campaign and self.running_bulk_campaign.get("status") == "running":
                messagebox.showwarning("Bulk Campaign", "Another campaign is currently running")
                return
            if not self.runtime.is_logged_in(self.active_bot):
                messagebox.showwarning("Bulk Campaign", "Login the active bot first")
                return

            text = message_box.get("1.0", "end").strip()
            if not text:
                messagebox.showwarning("Bulk Campaign", "Message is required")
                return

            selected = channel_list.curselection()
            if not selected:
                messagebox.showwarning("Bulk Campaign", "Select at least one channel")
                return

            try:
                delay_seconds = float(delay_var.get().strip())
            except ValueError:
                messagebox.showwarning("Bulk Campaign", "Delay must be numeric")
                return
            delay_seconds = max(0.0, min(10.0, delay_seconds))
            delay_ms = int(delay_seconds * 1000)

            channel_keys = []
            for idx in selected:
                if 0 <= idx < len(visible_channels):
                    channel_keys.append(visible_channels[idx])
            if not channel_keys:
                messagebox.showwarning("Bulk Campaign", "No valid channels selected")
                return

            payload = self.replace_mentions(text) if apply_mentions_var.get() else text

            campaign = {
                "id": self.next_bulk_campaign_id,
                "bot": self.active_bot,
                "started_at": datetime.utcnow().isoformat() + "Z",
                "ended_at": "",
                "status": "running",
                "total_channels": len(channel_keys),
                "sent": 0,
                "failed": 0,
                "target_channels": channel_keys,
                "message_preview": payload[:120],
                "errors": [],
            }
            self.next_bulk_campaign_id += 1
            self.bulk_history.append(campaign)
            self.bulk_history = self.bulk_history[-MAX_BULK_HISTORY_ITEMS:]
            self.running_bulk_campaign = campaign
            self.save_state()
            self.add_activity(f"Started bulk campaign #{campaign['id']} to {len(channel_keys)} channels")

            def send_next(index):
                if campaign.get("status") == "canceled":
                    progress_var.set(f"Campaign #{campaign['id']} canceled ({campaign['sent']} sent, {campaign['failed']} failed)")
                    self.running_bulk_campaign = None
                    self.save_state()
                    self.refresh_all_views()
                    return

                if index >= len(channel_keys):
                    campaign["status"] = "completed"
                    campaign["ended_at"] = datetime.utcnow().isoformat() + "Z"
                    progress_var.set(f"Campaign #{campaign['id']} completed ({campaign['sent']} sent, {campaign['failed']} failed)")
                    self.running_bulk_campaign = None
                    self.set_status(f"Campaign #{campaign['id']} complete")
                    self.add_activity(f"Completed bulk campaign #{campaign['id']}")
                    self.save_state()
                    self.refresh_all_views()
                    return

                channel_name = channel_keys[index]
                channel_id = self.channels.get(channel_name)
                if not channel_id:
                    campaign["failed"] += 1
                    campaign["errors"].append(f"Missing channel id for {channel_name}")
                elif not self.runtime.is_logged_in(campaign["bot"]):
                    campaign["failed"] += 1
                    campaign["errors"].append(f"Bot offline for {channel_name}")
                else:
                    self.runtime.send_message(campaign["bot"], payload, channel_id)
                    campaign["sent"] += 1
                    write_log_line(channel_name, f"You(campaign #{campaign['id']}): {payload}")
                    if channel_name == self.active_channel and campaign["bot"] == self.active_bot:
                        self.append_chat_line(f"[{self.active_bot}] You(campaign #{campaign['id']}): {payload}")

                done = campaign["sent"] + campaign["failed"]
                progress_var.set(f"Campaign #{campaign['id']} progress: {done}/{campaign['total_channels']} (sent {campaign['sent']} failed {campaign['failed']})")
                self.set_status(progress_var.get())
                self.save_state()
                self.root.after(delay_ms, lambda: send_next(index + 1))

            send_next(0)

        button_row = ttk.Frame(dialog, style="Card.TFrame")
        button_row.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(button_row, text="Start Campaign", style="App.TButton", command=start_campaign).pack(side="left")
        ttk.Button(button_row, text="Cancel Running", style="App.TButton", command=cancel_running_campaign).pack(side="left", padx=8)
        ttk.Button(button_row, text="Export History", style="App.TButton", command=export_bulk_history).pack(side="left")
        ttk.Button(button_row, text="Close", style="App.TButton", command=dialog.destroy).pack(side="left", padx=8)

        filter_var.trace_add("write", refill_channels)
        refill_channels()

    def send_embed_dialog(self):
        if not self.active_bot or self.active_bot not in self.bots:
            messagebox.showwarning("Send Embed", "Select an active bot")
            return
        if not self.active_channel or self.active_channel not in self.channels:
            messagebox.showwarning("Send Embed", "Select an active channel")
            return

        _idx, selected_preset = self.get_selected_embed_preset()
        initial = dict(selected_preset) if selected_preset else {"name": "Quick Embed", "color": "#4A90E2"}
        embed_data = self.open_embed_editor("Send Embed", "Queue Embed", initial)
        if not embed_data:
            return

        try:
            color_value = int(embed_data["color"].replace("#", ""), 16)
        except ValueError:
            messagebox.showerror("Send Embed", "Color must be a valid hex value")
            return

        title_text = self.replace_mentions(embed_data.get("title", ""))
        description_text = self.replace_mentions(embed_data.get("description", ""))
        self.runtime.send_embed(
            self.active_bot,
            self.channels[self.active_channel],
            title_text,
            description_text,
            color_value,
            embed_data.get("footer", "").strip(),
        )
        self.append_chat_line(f"[{self.active_bot}] You(embed): {title_text}")
        write_log_line(self.active_channel, f"You(embed): {title_text} | {description_text}")
        self.set_status("Embed queued")
        self.add_activity(f"Queued embed for {self.active_bot} -> {self.active_channel}")

    def send_file_dialog(self):
        if not self.active_bot or self.active_bot not in self.bots:
            messagebox.showwarning("Send File", "Select an active bot")
            return
        if not self.active_channel or self.active_channel not in self.channels:
            messagebox.showwarning("Send File", "Select an active channel")
            return
        file_path = filedialog.askopenfilename(parent=self.root, title="Choose File to Send")
        if not file_path:
            return
        caption = simpledialog.askstring("Send File", "Caption (optional):", parent=self.root)
        if caption is None:
            caption = ""
        self.runtime.send_file(self.active_bot, self.channels[self.active_channel], file_path, self.replace_mentions(caption.strip()))
        file_name = os.path.basename(file_path)
        self.append_chat_line(f"[{self.active_bot}] You(file): {file_name}")
        write_log_line(self.active_channel, f"You(file): {file_name} | {caption.strip()}")
        self.set_status("File queued")
        self.add_activity(f"Queued file {file_name} for {self.active_bot} -> {self.active_channel}")

    def schedule_message(self):
        text = self.schedule_text_var.get().strip()
        delay = self.schedule_delay_var.get().strip()
        interval = self.schedule_interval_var.get().strip()
        repeat = self.schedule_repeat_var.get().strip()
        if not text:
            messagebox.showwarning("Schedule", "Message is required")
            return
        if not delay.isdigit():
            messagebox.showwarning("Schedule", "Delay must be numeric seconds")
            return
        if not interval.isdigit():
            messagebox.showwarning("Schedule", "Repeat interval must be numeric seconds")
            return
        if not repeat.isdigit() or int(repeat) < 1:
            messagebox.showwarning("Schedule", "Total sends must be at least 1")
            return
        if not self.active_bot or self.active_bot not in self.bots:
            messagebox.showwarning("Schedule", "Select an active bot")
            return
        if not self.active_channel or self.active_channel not in self.channels:
            messagebox.showwarning("Schedule", "Select an active channel")
            return

        interval_seconds = int(interval)
        total_sends = int(repeat)
        if total_sends > 1 and interval_seconds <= 0:
            messagebox.showwarning("Schedule", "Repeat interval must be greater than 0 when total sends is more than 1")
            return

        job = {
            "id": self.next_schedule_id,
            "bot": self.active_bot,
            "channel_name": self.active_channel,
            "channel_id": self.channels[self.active_channel],
            "text": self.replace_mentions(text),
            "run_at": datetime.utcnow() + timedelta(seconds=int(delay)),
            "interval_seconds": interval_seconds,
            "remaining_runs": total_sends,
            "paused": False,
            "max_retries": 5,
            "run_count": 0,
            "fail_count": 0,
            "retry_count": 0,
            "last_run_at": "",
            "last_result": "pending",
            "last_error": "",
        }
        self.next_schedule_id += 1
        self.scheduled_jobs.append(job)
        self.schedule_text_var.set("")
        self.schedule_interval_var.set("0")
        self.schedule_repeat_var.set("1")
        self.save_state()
        self.refresh_schedule_list()
        self.set_status(f"Scheduled message #{job['id']}")
        self.add_activity(f"Created schedule #{job['id']}")

    def edit_selected_schedule(self):
        sel = self.schedule_list.curselection()
        if not sel:
            messagebox.showinfo("Scheduler", "Select a scheduled item first")
            return
        idx = sel[0]
        if not (0 <= idx < len(self.scheduled_jobs)):
            return
        job = self.scheduled_jobs[idx]
        new_text = simpledialog.askstring("Edit Schedule", "Message:", initialvalue=job["text"], parent=self.root)
        if not new_text:
            return
        delay = simpledialog.askstring(
            "Edit Schedule",
            "Seconds from now for next send:",
            initialvalue=str(max(0, int((job["run_at"] - datetime.utcnow()).total_seconds()))),
            parent=self.root,
        )
        interval = simpledialog.askstring("Edit Schedule", "Repeat every seconds:", initialvalue=str(job.get("interval_seconds", 0)), parent=self.root)
        repeats = simpledialog.askstring("Edit Schedule", "Remaining sends:", initialvalue=str(job.get("remaining_runs", 1)), parent=self.root)
        retries = simpledialog.askstring("Edit Schedule", "Max retries if bot is offline:", initialvalue=str(job.get("max_retries", 5)), parent=self.root)
        if not delay or not delay.isdigit() or not interval or not interval.isdigit() or not repeats or not repeats.isdigit() or not retries or not retries.isdigit():
            messagebox.showwarning("Edit Schedule", "All values must be numeric")
            return
        if int(repeats) > 1 and int(interval) <= 0:
            messagebox.showwarning("Edit Schedule", "Repeat interval must be greater than 0 when remaining sends is more than 1")
            return
        job["text"] = self.replace_mentions(new_text.strip())
        job["run_at"] = datetime.utcnow() + timedelta(seconds=int(delay))
        job["interval_seconds"] = int(interval)
        job["remaining_runs"] = int(repeats)
        job["max_retries"] = int(retries)
        self.save_state()
        self.refresh_schedule_list()
        self.add_activity(f"Edited schedule #{job['id']}")

    def duplicate_selected_schedule(self):
        sel = self.schedule_list.curselection()
        if not sel:
            messagebox.showinfo("Scheduler", "Select a scheduled item first")
            return
        idx = sel[0]
        if not (0 <= idx < len(self.scheduled_jobs)):
            return
        source = self.scheduled_jobs[idx]
        job = {
            "id": self.next_schedule_id,
            "bot": source["bot"],
            "channel_name": source["channel_name"],
            "channel_id": source["channel_id"],
            "text": source["text"],
            "run_at": datetime.utcnow() + timedelta(seconds=30),
            "interval_seconds": source.get("interval_seconds", 0),
            "remaining_runs": source.get("remaining_runs", 1),
            "paused": bool(source.get("paused", False)),
            "max_retries": source.get("max_retries", 5),
            "run_count": 0,
            "fail_count": 0,
            "retry_count": 0,
            "last_run_at": "",
            "last_result": "pending",
            "last_error": "",
        }
        self.next_schedule_id += 1
        self.scheduled_jobs.append(job)
        self.save_state()
        self.refresh_schedule_list()
        self.add_activity(f"Duplicated schedule #{source['id']} to #{job['id']}")

    def toggle_selected_schedule_pause(self):
        sel = self.schedule_list.curselection()
        if not sel:
            messagebox.showinfo("Scheduler", "Select a scheduled item first")
            return
        idx = sel[0]
        if not (0 <= idx < len(self.scheduled_jobs)):
            return
        job = self.scheduled_jobs[idx]
        job["paused"] = not bool(job.get("paused", False))
        self.save_state()
        self.refresh_schedule_list()
        if job["paused"]:
            self.set_status(f"Paused schedule #{job['id']}")
            self.add_activity(f"Paused schedule #{job['id']}")
        else:
            self.set_status(f"Resumed schedule #{job['id']}")
            self.add_activity(f"Resumed schedule #{job['id']}")

    def show_selected_schedule_report(self):
        sel = self.schedule_list.curselection()
        if not sel:
            messagebox.showinfo("Scheduler", "Select a scheduled item first")
            return
        idx = sel[0]
        if not (0 <= idx < len(self.scheduled_jobs)):
            return
        job = self.scheduled_jobs[idx]
        report = (
            f"Schedule #{job['id']}\n"
            f"Bot: {job['bot']}\n"
            f"Channel: {job['channel_name']} ({job['channel_id']})\n"
            f"State: {'Paused' if job.get('paused', False) else 'Running'}\n"
            f"Next run: {job['run_at'].isoformat()}Z\n"
            f"Interval: {job.get('interval_seconds', 0)}s\n"
            f"Remaining sends: {job.get('remaining_runs', 1)}\n"
            f"Max retries: {job.get('max_retries', 5)}\n"
            f"Run count: {job.get('run_count', 0)}\n"
            f"Fail count: {job.get('fail_count', 0)}\n"
            f"Current retry count: {job.get('retry_count', 0)}\n"
            f"Last run at: {job.get('last_run_at', '') or '(never)'}\n"
            f"Last result: {job.get('last_result', 'pending')}\n"
            f"Last error: {job.get('last_error', '') or '(none)'}\n"
            f"Message:\n{job.get('text', '')}"
        )
        messagebox.showinfo("Schedule Report", report)

    def export_schedule_jobs(self):
        path = filedialog.asksaveasfilename(
            parent=self.root,
            title="Export Schedule Jobs",
            defaultextension=".json",
            filetypes=[("JSON", "*.json")],
            initialfile="discord_bot_studio_schedules.json",
        )
        if not path:
            return
        payload = [serialize_schedule_job(job) for job in self.scheduled_jobs]
        try:
            with open(path, "w", encoding="utf8") as f:
                json.dump(payload, f, indent=2)
        except OSError as exc:
            messagebox.showerror("Export Schedule Jobs", f"Failed to write file: {exc}")
            return
        self.set_status(f"Schedules exported: {os.path.basename(path)}")
        self.add_activity(f"Exported schedule jobs {os.path.basename(path)}")

    def cancel_selected_schedule(self):
        sel = self.schedule_list.curselection()
        if not sel:
            messagebox.showinfo("Scheduler", "Select a scheduled item first")
            return
        idx = sel[0]
        if 0 <= idx < len(self.scheduled_jobs):
            job = self.scheduled_jobs.pop(idx)
            self.save_state()
            self.refresh_schedule_list()
            self.set_status(f"Cancelled schedule #{job['id']}")
            self.add_activity(f"Cancelled schedule #{job['id']}")

    def process_scheduled_jobs(self):
        now = datetime.utcnow()
        due, pending = [], []
        for job in self.scheduled_jobs:
            if job.get("paused", False):
                pending.append(job)
                continue
            if job["run_at"] <= now:
                due.append(job)
            else:
                pending.append(job)
        self.scheduled_jobs = pending

        for job in due:
            now_stamp = datetime.utcnow().isoformat() + "Z"
            if not self.runtime.is_logged_in(job["bot"]):
                job["fail_count"] = int(job.get("fail_count", 0)) + 1
                job["retry_count"] = int(job.get("retry_count", 0)) + 1
                job["last_run_at"] = now_stamp
                job["last_result"] = "retrying"
                job["last_error"] = "Bot is offline"
                if job["retry_count"] <= int(job.get("max_retries", 5)):
                    job["run_at"] = datetime.utcnow() + timedelta(seconds=SCHEDULE_RETRY_DELAY_SECONDS)
                    self.scheduled_jobs.append(job)
                    self.add_activity(f"Schedule #{job['id']} retry {job['retry_count']} queued")
                else:
                    job["last_result"] = "failed"
                    self.add_activity(f"Schedule #{job['id']} failed after max retries")
                continue

            self.runtime.send_message(job["bot"], job["text"], job["channel_id"])
            line = f"You(scheduled): {job['text']}"
            write_log_line(job["channel_name"], line)
            if job["bot"] == self.active_bot and job["channel_name"] == self.active_channel:
                self.append_chat_line(f"[{job['bot']}] {line}")

            job["run_count"] = int(job.get("run_count", 0)) + 1
            job["retry_count"] = 0
            job["last_run_at"] = now_stamp
            job["last_result"] = "queued"
            job["last_error"] = ""

            if job.get("remaining_runs", 1) > 1 and job.get("interval_seconds", 0) > 0:
                job["remaining_runs"] -= 1
                job["run_at"] = datetime.utcnow() + timedelta(seconds=job["interval_seconds"])
                self.scheduled_jobs.append(job)
            self.set_status(f"Sent scheduled message #{job['id']}")
            self.add_activity(f"Sent schedule #{job['id']}")

        self.save_state()
        self.refresh_schedule_list()
        self.root.after(1000, self.process_scheduled_jobs)

    def import_channels_from_discord(self):
        if not self.active_bot:
            messagebox.showinfo("Import Channels", "Select an active bot first")
            return
        if not self.runtime.is_logged_in(self.active_bot):
            messagebox.showinfo("Import Channels", "Login the active bot first")
            return

        try:
            entries = self.runtime.fetch_channels(self.active_bot).result(timeout=12)
        except Exception as exc:
            messagebox.showerror("Import Channels", f"Failed to fetch channels: {exc}")
            return

        if not entries:
            messagebox.showinfo("Import Channels", "No readable text channels found")
            return

        dialog = tk.Toplevel(self.root)
        dialog.title("Import Discord Channels")
        dialog.geometry("760x520")

        ttk.Label(dialog, text="Select one or more channels to import:").pack(anchor="w", padx=10, pady=(10, 6))
        lb = tk.Listbox(dialog, selectmode=tk.EXTENDED, bg="#0F182A", fg="#E8EEF8", selectbackground="#2E4D83", font=("Consolas", 10))
        lb.pack(fill="both", expand=True, padx=10, pady=(0, 8))

        rendered = []
        for guild_name, channel_name, channel_id in entries:
            line = f"{guild_name} / #{channel_name} ({channel_id})"
            lb.insert(tk.END, line)
            rendered.append((guild_name, channel_name, channel_id))

        def do_import():
            selected = lb.curselection()
            if not selected:
                messagebox.showinfo("Import Channels", "Select at least one channel")
                return
            added = 0
            for idx in selected:
                guild_name, channel_name, channel_id = rendered[idx]
                base = f"{guild_name}-{channel_name}"
                key = base
                suffix = 2
                while key in self.channels:
                    key = f"{base}-{suffix}"
                    suffix += 1
                self.channels[key] = channel_id
                added += 1
            save_kv_file(CHANNEL_FILE, self.channels)
            if not self.active_channel and self.channels:
                self.active_channel = next(iter(self.channels.keys()))
            self.save_state()
            self.refresh_all_views()
            self.load_recent_log_to_chat()
            self.set_status(f"Imported {added} channel(s)")
            self.add_activity(f"Imported {added} channels from Discord")
            dialog.destroy()

        action_row = ttk.Frame(dialog)
        action_row.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(action_row, text="Import Selected", style="App.TButton", command=do_import).pack(side="left")
        ttk.Button(action_row, text="Close", style="App.TButton", command=dialog.destroy).pack(side="left", padx=8)

    def choose_theme(self):
        dialog = tk.Toplevel(self.root)
        dialog.title("Choose Theme")
        dialog.geometry("340x140")
        dialog.transient(self.root)
        dialog.grab_set()

        ttk.Label(dialog, text="Theme:").pack(anchor="w", padx=10, pady=(10, 4))
        var = tk.StringVar(value=self.theme_name)
        combo = ttk.Combobox(dialog, state="readonly", values=list(THEMES.keys()), textvariable=var)
        combo.pack(fill="x", padx=10)

        def apply_selected():
            self.theme_name = var.get()
            self.save_state()
            self.rebuild_ui_for_theme()
            dialog.destroy()

        row = ttk.Frame(dialog)
        row.pack(fill="x", padx=10, pady=10)
        ttk.Button(row, text="Apply", style="App.TButton", command=apply_selected).pack(side="left")
        ttk.Button(row, text="Cancel", style="App.TButton", command=dialog.destroy).pack(side="left", padx=8)

    def rebuild_ui_for_theme(self):
        for child in self.root.winfo_children():
            child.destroy()
        self.build_styles()
        self.build_ui()
        self.refresh_all_views()
        self.set_status(f"Theme changed to {self.theme_name}")

    def export_backup(self):
        path = filedialog.asksaveasfilename(
            parent=self.root,
            title="Export Backup",
            defaultextension=".json",
            filetypes=[("JSON", "*.json")],
            initialfile="discord_bot_studio_backup.json",
        )
        if not path:
            return
        payload = {
            "version": 1,
            "bots": self.bots,
            "channels": self.channels,
            "members": self.members,
            "settings": self.settings,
            "exported_at": datetime.utcnow().isoformat() + "Z",
        }
        try:
            with open(path, "w", encoding="utf8") as f:
                json.dump(payload, f, indent=2)
        except OSError as exc:
            messagebox.showerror("Export Backup", f"Failed to write file: {exc}")
            return
        self.set_status(f"Backup exported: {os.path.basename(path)}")
        self.add_activity(f"Exported backup {os.path.basename(path)}")

    def import_backup(self):
        path = filedialog.askopenfilename(
            parent=self.root,
            title="Import Backup",
            filetypes=[("JSON", "*.json")],
        )
        if not path:
            return
        try:
            with open(path, "r", encoding="utf8") as f:
                payload = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            messagebox.showerror("Import Backup", f"Failed to read file: {exc}")
            return

        bots = payload.get("bots")
        channels = payload.get("channels")
        members = payload.get("members")
        settings = payload.get("settings", {})
        if not isinstance(bots, dict) or not isinstance(channels, dict) or not isinstance(members, dict):
            messagebox.showerror("Import Backup", "Invalid backup format")
            return

        if not messagebox.askyesno("Import Backup", "This will replace current data. Continue?"):
            return

        self.bots = bots
        self.channels = channels
        self.members = members
        self.settings = settings if isinstance(settings, dict) else {}
        self.theme_name = self.settings.get("theme", "Midnight")
        if self.theme_name not in THEMES:
            self.theme_name = "Midnight"
        self.templates = normalize_templates(self.settings.get("templates", DEFAULT_TEMPLATES[:]))
        self.embed_presets = normalize_embed_presets(self.settings.get("embed_presets", DEFAULT_EMBED_PRESETS[:]))
        self.scheduled_jobs = deserialize_schedule_jobs(self.settings.get("scheduled_jobs", []))
        self.next_schedule_id = max((job["id"] for job in self.scheduled_jobs), default=0) + 1
        self.activity_feed = [str(item) for item in self.settings.get("activity_feed", [])[-MAX_ACTIVITY_ITEMS:]]
        self.favorite_bots = [str(item) for item in self.settings.get("favorite_bots", []) if isinstance(item, str)]
        self.favorite_channels = [str(item) for item in self.settings.get("favorite_channels", []) if isinstance(item, str)]
        self.notes_text = str(self.settings.get("notes_text", ""))
        self.draft_text = str(self.settings.get("draft_text", ""))
        self.bulk_history = [item for item in self.settings.get("bulk_history", []) if isinstance(item, dict)]
        self.bulk_history = self.bulk_history[-MAX_BULK_HISTORY_ITEMS:]
        self.next_bulk_campaign_id = max((int(item.get("id", 0)) for item in self.bulk_history), default=0) + 1
        self.running_bulk_campaign = None

        self.active_bot = self.settings.get("preferred_bot")
        if self.active_bot not in self.bots:
            self.active_bot = next(iter(self.bots.keys()), None)
        self.active_channel = self.settings.get("active_channel")
        if self.active_channel not in self.channels:
            self.active_channel = next(iter(self.channels.keys()), None)

        save_kv_file(BOT_FILE, self.bots)
        save_kv_file(CHANNEL_FILE, self.channels)
        save_kv_file(MEMBER_FILE, self.members)
        self.save_state()
        self.rebuild_ui_for_theme()
        self.load_recent_log_to_chat()
        self.set_status(f"Backup imported: {os.path.basename(path)}")
        self.add_activity(f"Imported backup {os.path.basename(path)}")

    def load_recent_log_to_chat(self):
        self.clear_chat()
        if not self.active_channel:
            return
        lines = read_log_lines(self.active_channel)[-40:]
        for line in lines:
            self.append_chat_line(line)

    def maybe_run_startup_wizard(self):
        if self.settings.get("startup_wizard_completed"):
            return
        if self.bots and self.channels:
            self.settings["startup_wizard_completed"] = True
            self.save_state()
            return

        if not messagebox.askyesno(
            "Setup Wizard",
            "Open a quick setup wizard to add your first bot and channel?",
            parent=self.root,
        ):
            return

        if not self.bots:
            self.add_bot()
        if not self.channels:
            self.add_channel()
        if self.active_bot and self.active_channel:
            if messagebox.askyesno("Setup Wizard", "Login the active bot now?", parent=self.root):
                self.login_selected_bot()
        self.settings["startup_wizard_completed"] = True
        self.save_state()
        self.add_activity("Completed startup wizard")

    def force_startup_wizard(self):
        self.settings["startup_wizard_completed"] = False
        self.save_state()
        self.maybe_run_startup_wizard()

    def append_chat_line(self, line):
        self.chat_box.config(state="normal")
        self.chat_box.insert("end", line + "\n")
        self.chat_box.see("end")
        self.chat_box.config(state="disabled")

    def clear_chat(self):
        self.chat_box.config(state="normal")
        self.chat_box.delete("1.0", "end")
        self.chat_box.config(state="disabled")

    def show_log_dialog(self):
        if not self.active_channel:
            messagebox.showinfo("Show Log", "No active channel selected")
            return
        lines = read_log_lines(self.active_channel)
        if not lines:
            messagebox.showinfo("Show Log", "No log lines for this channel")
            return
        lines = lines[-MAX_LOG_PREVIEW_LINES:]

        win = tk.Toplevel(self.root)
        win.title(f"Log Preview - {self.active_channel}")
        win.geometry("820x520")
        box = tk.Text(win, bg="#0F182A", fg="#E8EEF8", font=("Consolas", 10), wrap="word")
        box.pack(fill="both", expand=True, padx=8, pady=8)
        start_num = max(1, len(read_log_lines(self.active_channel)) - len(lines) + 1)
        for idx, line in enumerate(lines, start=start_num):
            box.insert("end", f"{idx}: {line}\n")
        box.config(state="disabled")

    def show_channel_inspector_dialog(self):
        if not self.active_bot or self.active_bot not in self.bots:
            messagebox.showinfo("Channel Inspector", "Select an active bot first")
            return
        if not self.active_channel or self.active_channel not in self.channels:
            messagebox.showinfo("Channel Inspector", "Select an active channel first")
            return
        if not self.runtime.is_logged_in(self.active_bot):
            messagebox.showinfo("Channel Inspector", "Login the active bot first")
            return

        channel_id = self.channels[self.active_channel]
        dialog = tk.Toplevel(self.root)
        dialog.title(f"Channel Inspector - {self.active_channel}")
        dialog.geometry("960x640")

        top = ttk.Frame(dialog, style="Card.TFrame")
        top.pack(fill="x", padx=10, pady=(10, 6))
        info_var = tk.StringVar(value="Loading channel details...")
        ttk.Label(top, textvariable=info_var, style="Meta.TLabel", justify="left").pack(side="left", anchor="w")

        controls = ttk.Frame(dialog, style="Card.TFrame")
        controls.pack(fill="x", padx=10, pady=(0, 6))
        ttk.Label(controls, text="Recent messages:", style="Meta.TLabel").pack(side="left")
        limit_var = tk.StringVar(value="30")
        ttk.Entry(controls, textvariable=limit_var, width=6).pack(side="left", padx=(8, 8))

        message_box = tk.Text(dialog, wrap="word", font=("Consolas", 10))
        message_box.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        message_box.config(state="disabled")
        snapshot_cache = {"meta": {}, "messages": []}

        def render_snapshot(data):
            if data.get("error"):
                messagebox.showerror("Channel Inspector", f"Failed to fetch snapshot: {data['error']}", parent=dialog)
                return
            meta = data.get("meta", {})
            snapshot_cache["meta"] = dict(meta)
            info_var.set(
                f"Guild: {meta.get('guild', 'Unknown')}\n"
                f"Channel: #{meta.get('channel_name', 'unknown')} ({meta.get('channel_id', channel_id)})\n"
                f"Members: {meta.get('member_count', 0)} | Slowmode: {meta.get('slowmode_delay', 0)}s | NSFW: {meta.get('nsfw', False)}\n"
                f"Topic: {meta.get('topic', '') or '(none)'}"
            )
            lines = data.get("messages", [])
            snapshot_cache["messages"] = list(lines)
            message_box.config(state="normal")
            message_box.delete("1.0", "end")
            if lines:
                for line in lines:
                    message_box.insert("end", line + "\n")
            else:
                message_box.insert("end", "No recent messages available\n")
            message_box.config(state="disabled")

        def load_snapshot():
            try:
                limit = int(limit_var.get().strip())
            except ValueError:
                messagebox.showwarning("Channel Inspector", "Message limit must be a number", parent=dialog)
                return
            limit = max(1, min(100, limit))
            info_var.set("Loading channel details...")
            try:
                data = self.runtime.fetch_channel_snapshot(self.active_bot, channel_id, limit).result(timeout=20)
            except Exception as exc:
                messagebox.showerror("Channel Inspector", f"Failed to fetch snapshot: {exc}", parent=dialog)
                return
            render_snapshot(data)
            self.add_activity(f"Inspected channel {self.active_channel}")

        def copy_channel_id():
            self.root.clipboard_clear()
            self.root.clipboard_append(str(channel_id))
            self.set_status("Channel ID copied")

        def export_snapshot_json():
            if not snapshot_cache.get("meta"):
                messagebox.showinfo("Channel Inspector", "No snapshot loaded yet", parent=dialog)
                return
            path = filedialog.asksaveasfilename(
                parent=dialog,
                title="Export Channel Snapshot (JSON)",
                defaultextension=".json",
                filetypes=[("JSON", "*.json")],
                initialfile=f"{safe_log_name(self.active_channel)}_snapshot.json",
            )
            if not path:
                return
            payload = {
                "exported_at": datetime.utcnow().isoformat() + "Z",
                "active_bot": self.active_bot,
                "active_channel": self.active_channel,
                "meta": snapshot_cache.get("meta", {}),
                "messages": snapshot_cache.get("messages", []),
            }
            try:
                with open(path, "w", encoding="utf8") as f:
                    json.dump(payload, f, indent=2)
            except OSError as exc:
                messagebox.showerror("Channel Inspector", f"Failed to export JSON: {exc}", parent=dialog)
                return
            self.set_status(f"Snapshot exported: {os.path.basename(path)}")
            self.add_activity(f"Exported channel snapshot JSON {os.path.basename(path)}")

        def export_snapshot_txt():
            if not snapshot_cache.get("meta"):
                messagebox.showinfo("Channel Inspector", "No snapshot loaded yet", parent=dialog)
                return
            path = filedialog.asksaveasfilename(
                parent=dialog,
                title="Export Channel Snapshot (TXT)",
                defaultextension=".txt",
                filetypes=[("Text", "*.txt")],
                initialfile=f"{safe_log_name(self.active_channel)}_snapshot.txt",
            )
            if not path:
                return
            meta = snapshot_cache.get("meta", {})
            lines = snapshot_cache.get("messages", [])
            try:
                with open(path, "w", encoding="utf8") as f:
                    f.write("Channel Snapshot\n")
                    f.write(f"Exported: {datetime.utcnow().isoformat()}Z\n")
                    f.write(f"Bot: {self.active_bot}\n")
                    f.write(f"Channel key: {self.active_channel}\n\n")
                    f.write("Metadata\n")
                    for key in ["guild", "channel_name", "channel_id", "member_count", "slowmode_delay", "nsfw", "topic"]:
                        f.write(f"- {key}: {meta.get(key, '')}\n")
                    f.write("\nRecent Messages\n")
                    for line in lines:
                        f.write(line + "\n")
            except OSError as exc:
                messagebox.showerror("Channel Inspector", f"Failed to export TXT: {exc}", parent=dialog)
                return
            self.set_status(f"Snapshot exported: {os.path.basename(path)}")
            self.add_activity(f"Exported channel snapshot TXT {os.path.basename(path)}")

        ttk.Button(controls, text="Refresh", style="App.TButton", command=load_snapshot).pack(side="left", padx=(0, 6))
        ttk.Button(controls, text="Copy Channel ID", style="App.TButton", command=copy_channel_id).pack(side="left", padx=(0, 6))
        ttk.Button(controls, text="Export JSON", style="App.TButton", command=export_snapshot_json).pack(side="left", padx=(0, 6))
        ttk.Button(controls, text="Export TXT", style="App.TButton", command=export_snapshot_txt).pack(side="left", padx=(0, 6))
        ttk.Button(
            controls,
            text="Use Topic As Draft",
            style="App.TButton",
            command=lambda: self.message_var.set(info_var.get().split("Topic: ", 1)[-1] if "Topic: " in info_var.get() else ""),
        ).pack(side="left", padx=(0, 6))
        ttk.Button(controls, text="Close", style="App.TButton", command=dialog.destroy).pack(side="left")

        load_snapshot()

    def export_current_log(self):
        if not self.active_channel:
            messagebox.showinfo("Export Log", "No active channel selected")
            return
        lines = read_log_lines(self.active_channel)
        if not lines:
            messagebox.showinfo("Export Log", "No log lines for this channel")
            return
        path = filedialog.asksaveasfilename(
            parent=self.root,
            title="Export Channel Log",
            defaultextension=".txt",
            filetypes=[("Text", "*.txt")],
            initialfile=f"{safe_log_name(self.active_channel)}_log.txt",
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf8") as f:
                f.write("\n".join(lines))
        except OSError as exc:
            messagebox.showerror("Export Log", f"Failed to write file: {exc}")
            return
        self.set_status(f"Exported log: {os.path.basename(path)}")
        self.add_activity(f"Exported channel log {os.path.basename(path)}")

    def search_log_dialog(self):
        if not self.active_channel:
            messagebox.showinfo("Search Log", "No active channel selected")
            return
        query = simpledialog.askstring("Search Log", "Search text:", parent=self.root)
        if not query:
            return
        matches = [(idx, line) for idx, line in enumerate(read_log_lines(self.active_channel), start=1) if query.lower() in line.lower()]
        if not matches:
            messagebox.showinfo("Search Log", "No matches found")
            return

        win = tk.Toplevel(self.root)
        win.title(f"Search Results - {self.active_channel}")
        win.geometry("820x520")
        theme = THEMES[self.theme_name]
        box = tk.Text(win, bg=theme["list_bg"], fg=theme["text_main"], font=("Consolas", 10), wrap="word")
        box.pack(fill="both", expand=True, padx=8, pady=8)
        for idx, line in matches:
            box.insert("end", f"{idx}: {line}\n")
        box.config(state="disabled")
        self.add_activity(f"Searched log for '{query}'")

    def export_activity_log(self):
        path = filedialog.asksaveasfilename(
            parent=self.root,
            title="Export Activity Log",
            defaultextension=".txt",
            filetypes=[("Text", "*.txt")],
            initialfile="discord_bot_studio_activity.txt",
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf8") as f:
                f.write("\n".join(self.activity_feed))
        except OSError as exc:
            messagebox.showerror("Export Activity", f"Failed to write file: {exc}")
            return
        self.set_status(f"Activity exported: {os.path.basename(path)}")
        self.add_activity(f"Exported activity log {os.path.basename(path)}")

    def show_status_dialog(self):
        connected = [name for name in self.bots if self.runtime.is_logged_in(name)]
        recurring = sum(1 for job in self.scheduled_jobs if job.get("remaining_runs", 1) > 1)
        running_campaign = "None"
        if self.running_bulk_campaign and self.running_bulk_campaign.get("status") == "running":
            running_campaign = f"#{self.running_bulk_campaign.get('id', '?')}"
        message = (
            f"Saved bots: {len(self.bots)}\n"
            f"Connected bots: {len(connected)}\n"
            f"Saved channels: {len(self.channels)}\n"
            f"Saved members: {len(self.members)}\n"
            f"Templates: {len(self.templates)}\n"
            f"Scheduled jobs: {len(self.scheduled_jobs)}\n"
            f"Recurring jobs: {recurring}\n"
            f"Bulk campaigns: {len(self.bulk_history)}\n"
            f"Running campaign: {running_campaign}\n"
            f"Theme: {self.theme_name}\n"
            f"Active bot: {self.active_bot}\n"
            f"Active channel: {self.active_channel}"
        )
        messagebox.showinfo("Status", message)

    def set_status(self, text):
        self.status_var.set(text)

    def poll_runtime_events(self):
        while True:
            try:
                event = self.event_queue.get_nowait()
            except queue.Empty:
                break

            kind = event.get("kind")
            payload = event.get("payload")

            if kind == "system":
                self.append_chat_line(f"[System] {payload}")
                self.set_status(str(payload))
                self.add_activity(str(payload))
            elif kind == "ready":
                bot_name = payload.get("bot_name")
                user = payload.get("user")
                self.append_chat_line(f"[System] [{bot_name}] Connected as {user}")
                self.set_status(f"{bot_name} connected")
                self.add_activity(f"{bot_name} connected as {user}")
            elif kind == "incoming":
                bot_name = payload.get("bot_name")
                channel_id = payload.get("channel_id")
                text = payload.get("text", "")
                channel_name = self.find_channel_name_by_id(channel_id)
                if channel_name:
                    write_log_line(channel_name, text)
                if bot_name == self.active_bot and channel_name == self.active_channel:
                    self.append_chat_line(f"[{bot_name}] {text}")
                self.add_activity(f"Incoming message on {channel_name or channel_id} via {bot_name}")

        self.refresh_all_views()
        self.root.after(120, self.poll_runtime_events)

    def find_channel_name_by_id(self, channel_id):
        for name, cid in self.channels.items():
            if str(cid) == str(channel_id):
                return name
        return None

    def on_close(self):
        self.save_state()
        self.runtime.shutdown()
        self.root.destroy()


def main():
    root = tk.Tk()
    DiscordDesktopApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()