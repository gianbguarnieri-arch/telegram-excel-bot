import os
import re
import json
import sqlite3
import secrets
import string
import logging
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List

import httpx
from fastapi import FastAPI, Request, Header
from fastapi.responses import HTMLResponse, RedirectResponse

from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# ============================ ENVs ================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()

SQLITE_PATH = os.getenv("SQLITE_PATH", "/tmp/db.sqlite")

GOOGLE_USE_OAUTH = os.getenv("GOOGLE_USE_OAUTH", "0") == "1"
GOOGLE_OAUTH_CLIENT_ID = os.getenv("GOOGLE_OAUTH_CLIENT_ID")
GOOGLE_OAUTH_CLIENT_SECRET = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET")
GOOGLE_OAUTH_REDIRECT_URI = os.getenv("GOOGLE_OAUTH_REDIRECT_URI")
GOOGLE_OAUTH_SCOPES = (os.getenv("GOOGLE_OAUTH_SCOPES") or
                       "https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/spreadsheets").split()
GOOGLE_TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "/tmp/google_oauth_token.json")
OAUTH_STATE_SECRET = os.getenv("OAUTH_STATE_SECRET", "change-me")

GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")

GS_TEMPLATE_ID = os.getenv("GS_TEMPLATE_ID")
GS_DEST_FOLDER_ID = os.getenv("GS_DEST_FOLDER_ID")

WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "🧾")
SHARE_LINK_ROLE = os.getenv("SHARE_LINK_ROLE", "writer")

SCOPES_SA = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# ============================ DB =================================
def _db():
    return sqlite3.connect(SQLITE_PATH)

def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def licenses_db_init():
    con = _db()
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS licenses (
        license_key TEXT PRIMARY KEY,
        status TEXT NOT NULL DEFAULT 'active',
        max_files INTEGER NOT NULL DEFAULT 1,
        expires_at TEXT,
        notes TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clients (
        chat_id TEXT PRIMARY KEY,
        license_key TEXT,
        email TEXT,
        file_scope TEXT,
        item_id TEXT,
        created_at TEXT,
        last_seen_at TEXT,
        FOREIGN KEY (license_key) REFERENCES licenses(license_key)
    )""")
    con.commit()
    con.close()

# ============================ Utils ================================
def _gen_key(prefix="GF"):
    alphabet = string.ascii_uppercase + string.digits
    part = lambda n: "".join(secrets.choice(alphabet) for _ in range(n))
    return f"{prefix}-{part(4)}-{part(4)}"

def create_license(days: Optional[int] = 30, max_files: int = 1,
                   notes: Optional[str] = None, custom_key: Optional[str] = None):
    key = custom_key or _gen_key()
    expires_at = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat(timespec="seconds") if days else None
    con = _db()
    con.execute("INSERT INTO licenses(license_key,status,max_files,expires_at,notes) VALUES(?,?,?,?,?)",
                (key, "active", max_files, expires_at, notes))
    con.commit()
    con.close()
    return key, expires_at

def get_license(license_key: str):
    con = _db()
    cur = con.execute("SELECT license_key,status,max_files,expires_at,notes FROM licenses WHERE license_key=?",
                      (license_key,))
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    return {"license_key": row[0], "status": row[1], "max_files": row[2], "expires_at": row[3], "notes": row[4]}

def is_license_valid(lic: dict):
    if not lic:
        return False, "Licença não encontrada."
    if lic["status"] != "active":
        return False, "Licença não está ativa."
    if lic["expires_at"]:
        try:
            if datetime.now(timezone.utc) > datetime.fromisoformat(lic["expires_at"]):
                return False, "Licença expirada."
        except Exception:
            return False, "Validade da licença inválida."
    return True, None

def bind_license_to_chat(chat_id: str, license_key: str):
    con = _db()
    cur = con.execute("SELECT chat_id FROM clients WHERE license_key=? AND chat_id<>? LIMIT 1",
                      (license_key, str(chat_id)))
    conflict = cur.fetchone()
    if conflict:
        con.close()
        return False, "Essa licença já foi usada por outro Telegram."
    con.execute("""INSERT OR IGNORE INTO clients(chat_id, created_at) VALUES(?,?)""",
                (str(chat_id), _now_iso()))
    con.execute("""UPDATE clients SET license_key=?, last_seen_at=? WHERE chat_id=?""",
                (license_key, _now_iso(), str(chat_id)))
    con.commit()
    con.close()
    return True, None

def get_client(chat_id: str):
    con = _db()
    cur = con.execute("""SELECT chat_id, license_key, email, file_scope, item_id, created_at, last_seen_at
                         FROM clients WHERE chat_id=?""", (str(chat_id),))
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    return {
        "chat_id": row[0],
        "license_key": row[1],
        "email": row[2],
        "file_scope": row[3],
        "item_id": row[4],
        "created_at": row[5],
        "last_seen_at": row[6],
    }

def set_client_email(chat_id: str, email: str):
    con = _db()
    con.execute("UPDATE clients SET email=?, last_seen_at=? WHERE chat_id=?",
                (email, _now_iso(), str(chat_id)))
    con.commit()
    con.close()

# ============================ Telegram ================================
async def tg_send(chat_id, text):
    async with httpx.AsyncClient(timeout=12) as client:
        try:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            )
        except Exception as e:
            logger.error(f"Falha Telegram: {e}")

# ============================ Admin Helpers ===========================
def _normalize_cmd(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()

def _is_admin(chat_id_val) -> bool:
    admin_env = (os.getenv("ADMIN_TELEGRAM_ID") or "").strip()
    return admin_env != "" and str(chat_id_val).strip() == admin_env

def _parse_licenca_nova_args(msg: str):
    parts = msg.split()
    if parts and "@" in parts[0]:
        parts[0] = parts[0].split("@")[0]
    custom_key = None
    days = 30
    email = None
    tail = parts[2:] if len(parts) >= 2 and parts[1] == "nova" else []
    for tok in tail:
        t = tok.strip()
        if t.isdigit():
            days = int(t)
        elif "@" in t and "." in t:
            email = t
        else:
            custom_key = t
    return custom_key, days, email

# ============================ Rotas ===================================
@app.on_event("startup")
def _startup():
    licenses_db_init()
    print(f"✅ DB pronto em {SQLITE_PATH}")
    print(f"Auth mode: {'OAuth' if GOOGLE_USE_OAUTH else 'Service Account'}")

@app.get("/ping")
def ping():
    return {"pong": True}

# ============================ Webhook =================================
@app.post("/telegram/webhook")
async def telegram_webhook(req: Request,
                           x_telegram_bot_api_secret_token: Optional[str] = Header(default=None)):
    try:
        body = await req.json()
        message = body.get("message") or {}
        chat_id = message.get("chat", {}).get("id")
        text = (message.get("text") or "").strip()
        if not chat_id or not text:
            return {"ok": True}
        chat_id_str = str(chat_id).strip()
        norm = _normalize_cmd(text)

        # === admin comandos ===
        if norm.startswith("/whoami"):
            if _is_admin(chat_id):
                await tg_send(chat_id, f"Você é admin ✅ (chat_id={chat_id})")
            else:
                await tg_send(chat_id, f"Você NÃO é admin ❌ (chat_id={chat_id})")
            return {"ok": True}

        if _is_admin(chat_id):
            if norm.startswith("/licenca info"):
                await tg_send(chat_id, "Admin OK ✅. Bot online.")
                return {"ok": True}
            if norm.startswith("/licenca") and "nova" in norm:
                custom_key, days, email = _parse_licenca_nova_args(norm)
                key, exp = create_license(days=None if days == 0 else days, custom_key=custom_key)
                validade = "vitalícia" if not exp else exp
                msg = f"🔑 *Licença criada:*\n`{key}`\n*Validade:* {validade}"
                if email:
                    msg += f"\n*Email:* {email}"
                await tg_send(chat_id, msg)
                return {"ok": True}

        # === start padrão ===
        if norm == "/start":
            await tg_send(chat_id, "Olá! Envie sua licença (ex: GF-XXXX-XXXX)")
            return {"ok": True}

        # === fallback ===
        await tg_send(chat_id, "❗ Comando não reconhecido.")
        return {"ok": True}

    except Exception as e:
        logger.exception("Erro no webhook")
        return {"ok": True}
