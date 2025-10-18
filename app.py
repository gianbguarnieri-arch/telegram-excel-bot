# app.py — FastAPI + Telegram + Google (OAuth) + Neon (Postgres)
# Pronto para colar e fazer deploy.

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

# Admins
ADMIN_TELEGRAM_ID = (os.getenv("ADMIN_TELEGRAM_ID") or "").strip()
ADMIN_TELEGRAM_IDS = [s.strip() for s in (os.getenv("ADMIN_TELEGRAM_IDS") or "").split(",") if s.strip()]
ADMIN_TELEGRAM_USERNAME = (os.getenv("ADMIN_TELEGRAM_USERNAME") or "").lstrip("@").strip()
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")  # ex: meu-segredo-admin-123

DATABASE_URL = os.getenv("DATABASE_URL")  # Neon: postgres://...sslmode=require

# Google (usar OAuth = 1, ou Service Account = 0)
GOOGLE_USE_OAUTH = os.getenv("GOOGLE_USE_OAUTH", "0") == "1"
GOOGLE_OAUTH_CLIENT_ID = os.getenv("GOOGLE_OAUTH_CLIENT_ID")
GOOGLE_OAUTH_CLIENT_SECRET = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET")
GOOGLE_OAUTH_REDIRECT_URI = os.getenv("GOOGLE_OAUTH_REDIRECT_URI")
GOOGLE_OAUTH_SCOPES = (os.getenv("GOOGLE_OAUTH_SCOPES") or
                       "https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/spreadsheets").split()
GOOGLE_TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "/tmp/google_oauth_token.json")

# (fallback para quem ainda usa SA em outro ambiente)
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON") or os.getenv("GOOGLE_SHEETS_CREDENTIALS")

GS_TEMPLATE_ID = os.getenv("GS_TEMPLATE_ID")
GS_DEST_FOLDER_ID = os.getenv("GS_DEST_FOLDER_ID") or os.getenv("DEST_FOLDER_ITEM_ID")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "🧾")
SHARE_LINK_ROLE = os.getenv("SHARE_LINK_ROLE", "writer")  # reader|commenter|writer

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

def get_license(license_key: str):
    with pool.connection() as con:
        cur = con.execute(
            "SELECT license_key,status,max_files,expires_at,notes FROM licenses WHERE license_key=%s",
            (license_key,)
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "license_key": row[0],
        "status": row[1],
        "max_files": row[2],
        "expires_at": row[3].isoformat(timespec="seconds") if row[3] else None,
        "notes": row[4],
    }

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
    lic = get_license(license_key)
    ok, err = is_license_valid(lic)
    if not ok:
        return False, err

    with pool.connection() as con:
        cur = con.execute("SELECT chat_id FROM clients WHERE license_key=%s AND chat_id<>%s LIMIT 1",
                          (license_key, str(chat_id)))
        if cur.fetchone():
            return False, "Essa licença já foi usada por outro Telegram."

        now = datetime.now(timezone.utc)
        con.execute("INSERT INTO clients(chat_id, created_at, last_seen_at) VALUES(%s,%s,%s) ON CONFLICT (chat_id) DO NOTHING",
                    (str(chat_id), now, now))
        con.execute("UPDATE clients SET license_key=%s, last_seen_at=%s WHERE chat_id=%s",
                    (license_key, now, str(chat_id)))
    return True, None

def set_client_email(chat_id: str, email: str):
    with pool.connection() as con:
        con.execute("UPDATE clients SET email=%s, last_seen_at=%s WHERE chat_id=%s",
                    (email, datetime.now(timezone.utc), str(chat_id)))

def get_client(chat_id: str):
    with pool.connection() as con:
        cur = con.execute(
            "SELECT chat_id, license_key, email, created_at, last_seen_at FROM clients WHERE chat_id=%s",
            (str(chat_id),)
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "chat_id": row[0],
        "license_key": row[1],
        "email": row[2],
        "created_at": row[3].isoformat(timespec="seconds"),
        "last_seen_at": row[4].isoformat(timespec="seconds"),
    }

def get_client_file_count(chat_id: str) -> int:
    with pool.connection() as con:
        cur = con.execute("SELECT COUNT(*) FROM client_files WHERE chat_id=%s", (str(chat_id),))
        return int(cur.fetchone()[0])

def save_client_file(chat_id: str, file_id: str):
    with pool.connection() as con:
        con.execute("INSERT INTO client_files(chat_id,file_id,created_at) VALUES(%s,%s,%s)",
                    (str(chat_id), file_id, datetime.now(timezone.utc)))

# ----------------- Admin helpers ----------------
def _is_admin(chat_id_val: str, username: Optional[str]) -> bool:
    cid = str(chat_id_val).strip()

    # 1) ID único
    if ADMIN_TELEGRAM_ID and cid == ADMIN_TELEGRAM_ID:
        return True
    # 2) Lista de IDs
    if ADMIN_TELEGRAM_IDS and cid in ADMIN_TELEGRAM_IDS:
        return True
    # 3) Username (env)
    if ADMIN_TELEGRAM_USERNAME and (username or "").lstrip("@").lower() == ADMIN_TELEGRAM_USERNAME.lower():
        return True
    # 4) Tabela admins (promovidos via /admin TOKEN)
    with pool.connection() as con:
        cur = con.execute("SELECT 1 FROM admins WHERE chat_id=%s LIMIT 1", (cid,))
        if cur.fetchone():
            return True
    return False

def _parse_licenca_nova_args(msg: str):
    # aceita: /licenca nova [dias|0] [email] [CUSTOMKEY]
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

# ----------------- Google APIs -------------
def _google_creds():
    if GOOGLE_USE_OAUTH:
        if not os.path.exists(GOOGLE_TOKEN_PATH):
            raise RuntimeError("Token OAuth não encontrado. Gere e suba o arquivo em GOOGLE_TOKEN_PATH.")
        with open(GOOGLE_TOKEN_PATH, "r") as f:
            data = json.load(f)
        creds = Credentials.from_authorized_user_info(data, GOOGLE_OAUTH_SCOPES)
        return creds
    else:
        if not GOOGLE_SA_JSON:
            raise RuntimeError("GOOGLE_SA_JSON/GOOGLE_SHEETS_CREDENTIALS ausente.")
        info = json.loads(GOOGLE_SA_JSON)
        creds = service_account.Credentials.from_service_account_info(info, scopes=GOOGLE_OAUTH_SCOPES)
        return creds

def gs_copy_template_and_share(user_hint_email: Optional[str] = None) -> Tuple[str, str]:
    creds = _google_creds()
    drive = build("drive", "v3", credentials=creds)
    sheets = build("sheets", "v4", credentials=creds)

    body = {"name": f"Cópia - {datetime.now().strftime('%Y-%m-%d %H-%M-%S')}",
            "parents": [GS_DEST_FOLDER_ID]}
    new_file = drive.files().copy(fileId=GS_TEMPLATE_ID, body=body, fields="id,name,webViewLink").execute()
    file_id = new_file["id"]

    drive.permissions().create(
        fileId=file_id,
        body={"type": "anyone", "role": SHARE_LINK_ROLE, "allowFileDiscovery": False},
        fields="id",
    ).execute()

    got = drive.files().get(fileId=file_id, fields="id,webViewLink").execute()
    return file_id, got.get("webViewLink")

# ----------------- FastAPI Lifecycle -------
@app.on_event("startup")
def _startup():
    db_init()
    logger.info("✅ DB pronto (Neon).")
    logger.info(f"Auth Google: {'OAuth' if GOOGLE_USE_OAUTH else 'Service Account'}")

@app.get("/ping")
def ping():
    return {"pong": True}

# ----------------- Webhook Telegram --------
@app.post("/telegram/webhook")
async def telegram_webhook(
    req: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(default=None)
):
    # Secret do webhook (se configurado)
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

        # Parser robusto (remove @SeuBot)
        raw = text.strip()
        parts = raw.split()
        cmd = parts[0].split("@")[0].lower() if parts else ""
        norm = _normalize_free(raw)

        logger.info({"from_id": chat_id, "from_username": username, "cmd": cmd})

        # --- HOTFIX: responder /whoami sempre, independente de ser admin ---
        if cmd == "/whoami":
            try:
                is_admin_now = _is_admin(str(chat_id), username)
            except Exception:
                is_admin_now = False
            who = (message.get("from") or {})
            uname = who.get("username") or "-"
            first = who.get("first_name") or "-"
            await tg_send(
                chat_id,
                (
                    "*whoami*\n"
                    f"• chat_id: `{chat_id}`\n"
                    f"• username: `{uname}`\n"
                    f"• first_name: `{first}`\n"
                    f"• admin: `{'true' if is_admin_now else 'false'}`"
                )
            )
            return {"ok": True}
        # --- FIM HOTFIX ---

        # ---------- Admin ----------
        if cmd == "/admin":
            if not ADMIN_TOKEN:
                await tg_send(chat_id, "ADMIN_TOKEN não configurado no servidor.")
                return {"ok": True}
            if len(parts) < 2:
                await tg_send(chat_id, "Uso: `/admin <TOKEN>`")
                return {"ok": True}
            if parts[1].strip() != ADMIN_TOKEN:
                await tg_send(chat_id, "Token inválido.")
                return {"ok": True}
            with pool.connection() as con:
                con.execute(
                    "INSERT INTO admins(chat_id, username, created_at) VALUES(%s,%s,%s) "
                    "ON CONFLICT (chat_id) DO NOTHING",
                    (str(chat_id), username, datetime.now(timezone.utc))
                )
            await tg_send(chat_id, "✅ Admin habilitado para este chat.")
            return {"ok": True}

        if _is_admin(str(chat_id), username):
            if cmd == "/licenca" and "nova" in norm:
                custom_key, days, email = _parse_licenca_nova_args(norm)
                key, exp = create_license(days=None if days == 0 else days, custom_key=custom_key)
                validade = "vitalícia" if not exp else exp
                msg = f"🔑 *Licença criada:*\n`{key}`\n*Validade:* {validade}"
                if email:
                    msg += f"\n*Email (sugestão):* {email}"
                await tg_send(chat_id, msg)
                return {"ok": True}

            if cmd == "/licenca" and "info" in norm:
                await tg_send(chat_id, "Admin OK ✅. Bot online.")
                return {"ok": True}

        # ---------- Usuário ----------
        if cmd == "/start":
            await tg_send(chat_id,
                          "Olá! Envie:\n"
                          "• `/licenca usar GF-XXXX-XXXX [email]` para vincular sua licença\n"
                          "• `/meus dados` para ver seu status\n"
                          "• `/planilha nova` para gerar sua planilha (após vincular a licença)")
            return {"ok": True}

        if cmd == "/licenca" and "usar" in norm:
            try:
                lic_key = parts[2]
            except Exception:
                await tg_send(chat_id, "Formato: `/licenca usar GF-XXXX-XXXX [email]`")
                return {"ok": True}

            ok, err = bind_license_to_chat(str(chat_id), lic_key)
            if not ok:
                await tg_send(chat_id, f"❌ {err}")
                return {"ok": True}

            if len(parts) >= 4 and "@" in parts[3]:
                set_client_email(str(chat_id), parts[3])

            await tg_send(chat_id, "✅ Licença vinculada com sucesso! Use `/planilha nova` para criar sua cópia.")
            return {"ok": True}

        if cmd == "/meus" and "dados" in norm:
            cli = get_client(str(chat_id))
            if not cli:
                await tg_send(chat_id, "Você ainda não está cadastrado. Use `/licenca usar ...`.")
                return {"ok": True}
            lic = get_license(cli["license_key"]) if cli["license_key"] else None
            count = get_client_file_count(str(chat_id))
            msg = [
                "*Seus dados:*",
                f"• chat_id: `{cli['chat_id']}`",
                f"• email: `{cli['email'] or '-'}'",
                f"• licença: `{cli['license_key'] or '-'}'",
                f"• arquivos gerados: `{count}`",
                f"• criado em: `{cli['created_at']}`",
                f"• último acesso: `{cli['last_seen_at']}`",
            ]
            if lic:
                msg += [
                    "*Licença:*",
                    f"• status: `{lic['status']}`",
                    f"• max_files: `{lic['max_files']}`",
                    f"• expira em: `{lic['expires_at'] or 'vitalícia'}`",
                    f"• notas: `{lic['notes'] or '-'}'",
                ]
            await tg_send(chat_id, "\n".join(msg))
            return {"ok": True}

        if cmd == "/planilha" and "nova" in norm:
            cli = get_client(str(chat_id))
            if not cli or not cli["license_key"]:
                await tg_send(chat_id, "Vincule sua licença primeiro: `/licenca usar GF-XXXX-XXXX [email]`")
                return {"ok": True}

            lic = get_license(cli["license_key"])
            ok, err = is_license_valid(lic)
            if not ok:
                await tg_send(chat_id, f"❌ {err}")
                return {"ok": True}

            current = get_client_file_count(str(chat_id))
            if current >= int(lic["max_files"]):
                await tg_send(chat_id, f"❌ Limite de arquivos atingido ({lic['max_files']}).")
                return {"ok": True}

            try:
                file_id, link = gs_copy_template_and_share(cli.get("email"))
            except Exception:
                logger.exception("Falha ao copiar/compartilhar planilha")
                await tg_send(chat_id, "❌ Erro ao gerar a planilha. Tente novamente em alguns minutos.")
                return {"ok": True}

            save_client_file(str(chat_id), file_id)
            await tg_send(chat_id, f"✅ Planilha criada!\n*File ID:* `{file_id}`\n*Acesse:* {link}")
            return {"ok": True}

        # ---------- Fallback ----------
        await tg_send(chat_id, "❗ Comando não reconhecido. Use `/start` para ajuda.")
        return {"ok": True}

    except Exception:
        logger.exception("Erro no webhook")
        return {"ok": True}
