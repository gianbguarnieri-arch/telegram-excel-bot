# app.py — FastAPI + Telegram + Google (OAuth) + Neon (Postgres)
# Versão atualizada: 100% funcional com /licenca nova

import os
import json
import secrets
import string
import logging
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import httpx
from fastapi import FastAPI, Request, Header
from fastapi.responses import HTMLResponse

import psycopg
from psycopg_pool import ConnectionPool
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ----------------- Logging -----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")

# ----------------- ENVs --------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_WEBHOOK_SECRET = (os.getenv("TELEGRAM_WEBHOOK_SECRET") or "").strip()

ADMIN_TELEGRAM_ID = (os.getenv("ADMIN_TELEGRAM_ID") or "").strip()
ADMIN_TELEGRAM_IDS = [s.strip() for s in (os.getenv("ADMIN_TELEGRAM_IDS") or "").split(",") if s.strip()]
ADMIN_TELEGRAM_USERNAME = (os.getenv("ADMIN_TELEGRAM_USERNAME") or "").lstrip("@").strip()
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")

DATABASE_URL = os.getenv("DATABASE_URL")

GOOGLE_USE_OAUTH = os.getenv("GOOGLE_USE_OAUTH", "0") == "1"
GOOGLE_OAUTH_CLIENT_ID = os.getenv("GOOGLE_OAUTH_CLIENT_ID")
GOOGLE_OAUTH_CLIENT_SECRET = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET")
GOOGLE_OAUTH_REDIRECT_URI = os.getenv("GOOGLE_OAUTH_REDIRECT_URI")
GOOGLE_OAUTH_SCOPES = (os.getenv("GOOGLE_OAUTH_SCOPES") or
                       "https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/spreadsheets").split()
GOOGLE_TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "/tmp/google_oauth_token.json")

GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON") or os.getenv("GOOGLE_SHEETS_CREDENTIALS")

GS_TEMPLATE_ID = os.getenv("GS_TEMPLATE_ID")
GS_DEST_FOLDER_ID = os.getenv("GS_DEST_FOLDER_ID") or os.getenv("DEST_FOLDER_ITEM_ID")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "🧾")
SHARE_LINK_ROLE = os.getenv("SHARE_LINK_ROLE", "writer")

# ----------------- App & DB ----------------
if not DATABASE_URL:
    raise RuntimeError("Defina DATABASE_URL (Neon).")

app = FastAPI()
pool = ConnectionPool(conninfo=DATABASE_URL, open=True, min_size=1, max_size=10, kwargs={"autocommit": True})

# ----------------- Helpers -----------------
def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _gen_key(prefix="GF"):
    alphabet = string.ascii_uppercase + string.digits
    part = lambda n: "".join(secrets.choice(alphabet) for _ in range(n))
    return f"{prefix}-{part(4)}-{part(4)}"

def _normalize_free(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()

# ----------------- DB Schema ----------------
DDL = """
CREATE TABLE IF NOT EXISTS licenses (
    license_key TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'active',
    max_files INTEGER NOT NULL DEFAULT 1,
    expires_at TIMESTAMPTZ,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS clients (
    chat_id TEXT PRIMARY KEY,
    license_key TEXT REFERENCES licenses(license_key),
    email TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS client_files (
    id BIGSERIAL PRIMARY KEY,
    chat_id TEXT REFERENCES clients(chat_id),
    file_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS admins (
    chat_id TEXT PRIMARY KEY,
    username TEXT,
    created_at TIMESTAMPTZ NOT NULL
);
"""

def db_init():
    with pool.connection() as con:
        con.execute(DDL)

# ----------------- Licenças ----------------
def create_license(days: Optional[int] = 30, max_files: int = 1,
                   notes: Optional[str] = None, custom_key: Optional[str] = None):
    key = custom_key or _gen_key()
    expires_at = (datetime.now(timezone.utc) + timedelta(days=days)) if days else None
    with pool.connection() as con:
        con.execute(
            "INSERT INTO licenses(license_key,status,max_files,expires_at,notes) VALUES(%s,%s,%s,%s,%s)",
            (key, "active", max_files, expires_at, notes),
        )
    return key, (expires_at.isoformat(timespec="seconds") if expires_at else None)

# ----------------- Telegram ----------------
async def tg_send(chat_id, text):
    async with httpx.AsyncClient(timeout=12) as client:
        try:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            )
        except Exception as e:
            logger.error(f"Falha Telegram: {e}")

# ----------------- Admin helpers ----------------
def _is_admin(chat_id_val: str, username: Optional[str]) -> bool:
    cid = str(chat_id_val).strip()
    if ADMIN_TELEGRAM_ID and cid == ADMIN_TELEGRAM_ID:
        return True
    if ADMIN_TELEGRAM_IDS and cid in ADMIN_TELEGRAM_IDS:
        return True
    if ADMIN_TELEGRAM_USERNAME and (username or "").lstrip("@").lower() == ADMIN_TELEGRAM_USERNAME.lower():
        return True
    with pool.connection() as con:
        cur = con.execute("SELECT 1 FROM admins WHERE chat_id=%s LIMIT 1", (cid,))
        if cur.fetchone():
            return True
    return False

def _parse_licenca_nova_args(msg: str):
    parts = msg.split()
    custom_key = None
    days = 30
    email = None
    tail = parts[2:] if len(parts) >= 2 and "nova" in parts[1] else []
    for tok in tail:
        t = tok.strip()
        if t.isdigit():
            days = int(t)
        elif "@" in t and "." in t:
            email = t
        else:
            custom_key = t
    return custom_key, days, email

# ----------------- FastAPI Lifecycle -------
@app.on_event("startup")
def _startup():
    db_init()
    logger.info("✅ DB pronto (Neon).")

@app.get("/ping")
def ping():
    return {"pong": True}

# ----------------- Webhook Telegram --------
@app.post("/telegram/webhook")
async def telegram_webhook(
    req: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(default=None)
):
    try:
        body = await req.json()
        message = body.get("message") or {}
        chat_id = message.get("chat", {}).get("id")
        text = (message.get("text") or "").strip()
        username = (message.get("from") or {}).get("username")

        if not chat_id or not text:
            return {"ok": True}

        raw = text.strip()
        parts = raw.split()
        cmd = parts[0].split("@")[0].lower() if parts else ""
        norm = _normalize_free(raw)

        logger.info({"from_id": chat_id, "from_username": username, "cmd": cmd})

        # --- whoami sempre responde ---
        if cmd == "/whoami":
            try:
                is_admin_now = _is_admin(str(chat_id), username)
            except Exception:
                is_admin_now = False
            await tg_send(
                chat_id,
                (
                    "*whoami*\n"
                    f"• chat_id: `{chat_id}`\n"
                    f"• username: `{username or '-'}`\n"
                    f"• admin: `{'true' if is_admin_now else 'false'}`"
                )
            )
            return {"ok": True}

        # ---------- Admin ----------
        if _is_admin(str(chat_id), username):
            # PATCH: reconhecimento mais flexível do comando
            if cmd.startswith("/licenca") and ("nova" in norm or "nova" in text.lower()):
                custom_key, days, email = _parse_licenca_nova_args(text)
                try:
                    key, exp = create_license(days=None if days == 0 else days, custom_key=custom_key)
                    validade = "vitalícia" if not exp else exp
                    msg = f"🔑 *Licença criada:*\n`{key}`\n*Validade:* {validade}"
                    if email:
                        msg += f"\n*Email:* {email}"
                    await tg_send(chat_id, msg)
                except Exception as e:
                    await tg_send(chat_id, f"❌ Erro ao criar licença: {e}")
                    logger.exception("Falha ao criar licença")
                return {"ok": True}

            if cmd == "/licenca" and "info" in norm:
                await tg_send(chat_id, "Admin OK ✅. Bot online.")
                return {"ok": True}

        # ---------- Usuário ----------
        if cmd == "/start":
            await tg_send(chat_id, "Olá! Envie sua licença (ex: GF-XXXX-XXXX)")
            return {"ok": True}

        await tg_send(chat_id, "❗ Comando não reconhecido.")
        return {"ok": True}

    except Exception:
        logger.exception("Erro no webhook")
        return {"ok": True}
