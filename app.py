# app.py — FastAPI + Telegram + Google (OAuth) + Neon (Postgres)
# Versão com pool/SSL/keepalive + retry e integração opcional com Sheets (Licenças A:F)

import os
import json
import time
import secrets
import string
import logging
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import httpx
from fastapi import FastAPI, Request, Header

import psycopg
from psycopg_pool import ConnectionPool
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

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

DATABASE_URL = os.getenv("DATABASE_URL")  # deve terminar com ?sslmode=require

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

# --- SHEETS (licenças) ---
LICENSE_SHEET_ID = os.getenv("LICENSE_SHEET_ID")
LICENSE_SHEET_TAB = os.getenv("LICENSE_SHEET_TAB", "Licencas")
LICENSE_SHEET_RANGE = os.getenv("LICENSE_SHEET_RANGE", f"{LICENSE_SHEET_TAB}!A:F")
SHEET_START_ROW = int(os.getenv("SHEET_START_ROW", "2"))

# ----------------- App & DB ----------------
if not DATABASE_URL:
    raise RuntimeError("Defina DATABASE_URL (Neon).")

app = FastAPI()

# Pool com SSL/keepalive + timeouts
pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,
    max_size=10,
    max_lifetime=300,   # recicla conexões antigas
    max_idle=90,        # fecha ociosas
    timeout=30,         # espera por conexão no pool
    open=True,
    kwargs={
        "autocommit": True,
        "sslmode": "require",
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    },
)

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

# Exec com retry para quedas SSL/timeout
def _exec_with_retry(sql: str, params: tuple = ()):
    last_exc = None
    for attempt in range(3):
        try:
            with pool.connection() as con:
                con.execute(sql, params)
            return
        except Exception as e:
            last_exc = e
            logger.warning(f"DB retry {attempt+1}/3: {e}")
            try:
                pool.check()
            except Exception:
                pass
            time.sleep(0.3 * (attempt + 1))
    raise last_exc

# ----------------- Google APIs -------------
def _google_creds():
    if GOOGLE_USE_OAUTH:
        if not os.path.exists(GOOGLE_TOKEN_PATH):
            raise RuntimeError("Token OAuth não encontrado em GOOGLE_TOKEN_PATH.")
        with open(GOOGLE_TOKEN_PATH, "r") as f:
            data = json.load(f)
        return Credentials.from_authorized_user_info(data, GOOGLE_OAUTH_SCOPES)
    else:
        if not GOOGLE_SA_JSON:
            raise RuntimeError("GOOGLE_SA_JSON/GOOGLE_SHEETS_CREDENTIALS ausente.")
        info = json.loads(GOOGLE_SA_JSON)
        return service_account.Credentials.from_service_account_info(info, scopes=GOOGLE_OAUTH_SCOPES)

def _sheets_client():
    if not LICENSE_SHEET_ID:
        return None
    creds = _google_creds()
    return build("sheets", "v4", credentials=creds).spreadsheets()

def _license_row_for_sheet(*, key: str, days: Optional[int], starts_at: datetime,
                           expires_at: Optional[datetime], email: Optional[str], status: str):
    validade = "vitalícia" if not days or days == 0 or not expires_at else str(days)
    inicio = starts_at.strftime("%Y-%m-%d %H:%M:%S")
    fim = expires_at.strftime("%Y-%m-%d %H:%M:%S") if expires_at else ""
    return [key, validade, inicio, fim, (email or ""), status]

def gs_append_license_row(*, key: str, days: Optional[int], email: Optional[str],
                          created_at: datetime, expires_at: Optional[datetime], status: str = "active"):
    sc = _sheets_client()
    if not sc:
        return
    body = {"values": [
        _license_row_for_sheet(key=key, days=days, starts_at=created_at,
                               expires_at=expires_at, email=email, status=status)
    ]}
    sc.values().append(
        spreadsheetId=LICENSE_SHEET_ID,
        range=LICENSE_SHEET_RANGE,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()

# ----------------- Licenças ----------------
def create_license(days: Optional[int] = 30, max_files: int = 1,
                   notes: Optional[str] = None, custom_key: Optional[str] = None,
                   email_for_sheet: Optional[str] = None):
    """Cria licença no banco (com retry) e registra na planilha (A:F) se configurada."""
    key = custom_key or _gen_key()
    created_at = datetime.now(timezone.utc)
    expires_at = (created_at + timedelta(days=days)) if days else None

    _exec_with_retry(
        "INSERT INTO licenses(license_key,status,max_files,expires_at,notes) VALUES(%s,%s,%s,%s,%s)",
        (key, "active", max_files, expires_at, notes),
    )

    # opcional: escrever na planilha Licenças (A:F)
    try:
        gs_append_license_row(
            key=key, days=days, email=email_for_sheet,
            created_at=created_at, expires_at=expires_at, status="active"
        )
    except Exception:
        logger.exception("Falha ao escrever licença no Sheets (ignorado).")

    return key, (expires_at.isoformat(timespec='seconds') if expires_at else None)

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

# ----------------- FastAPI Lifecycle -------
@app.on_event("startup")
def _startup():
    db_init()
    logger.info("✅ DB pronto (Neon). Pool com SSL/keepalive ativo.")

@app.get("/ping")
def ping():
    return {"pong": True}

# ----------------- Webhook Telegram --------
@app.post("/telegram/webhook")
async def telegram_webhook(
    req: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(default=None)
):
    # valida secret se existir
    if TELEGRAM_WEBHOOK_SECRET and (x_telegram_bot_api_secret_token or "").strip() != TELEGRAM_WEBHOOK_SECRET:
        logger.warning("Webhook rejeitado: secret inválido.")
        return {"ok": True}

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

        # /whoami sempre responde
        if cmd == "/whoami":
            try:
                is_admin_now = _is_admin(str(chat_id), username)
            except Exception:
                is_admin_now = False
            await tg_send(
                chat_id,
                (
                    "*whoami*\n"
                    f"• chatid: `{chat_id}`\n"
                    f"• username: `{username or '-'}`\n"
                    f"• admin: `{'true' if is_admin_now else 'false'}`"
                )
            )
            return {"ok": True}

        # Admin: promoção via token
        if cmd == "/admin":
            if not ADMIN_TOKEN:
                await tg_send(chat_id, "ADMIN_TOKEN não configurado.")
                return {"ok": True}
            if len(parts) < 2 or parts[1].strip() != ADMIN_TOKEN:
                await tg_send(chat_id, "Token inválido. Uso: `/admin <TOKEN>`")
                return {"ok": True}
            with pool.connection() as con:
                con.execute(
                    "INSERT INTO admins(chat_id, username, created_at) VALUES(%s,%s,%s) "
                    "ON CONFLICT (chat_id) DO NOTHING",
                    (str(chat_id), username, datetime.now(timezone.utc))
                )
            await tg_send(chat_id, "✅ Admin habilitado para este chat.")
            return {"ok": True}

        # ---------- Admin ----------
        if _is_admin(str(chat_id), username):

            if cmd.startswith("/licenca") and ("nova" in norm or "nova" in text.lower()):
                custom_key, days, email = _parse_licenca_nova_args(text)
                try:
                    key, exp = create_license(
                        days=None if days == 0 else days,
                        custom_key=custom_key,
                        email_for_sheet=email
                    )
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

        await tg_send(chat_id, "❗ Comando não reconhecido. Use `/whoami` ou `/start`.")
        return {"ok": True}

    except Exception:
        logger.exception("Erro no webhook")
        return {"ok": True}
