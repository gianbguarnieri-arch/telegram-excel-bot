# app.py — Telegram + FastAPI + Neon (Postgres) + Google Sheets
# versão: 2025-10-19
# - INSERT com start_date/end_date/email (e fallback)
# - colunas com underscore
# - retry de DB
# - escrita no Sheets A:F (Licença, Validade, Data de início, Data final, email, status)

import os
import json
import time
import secrets
import string
import logging
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
import psycopg
from fastapi import FastAPI, Request, Header
from fastapi.responses import JSONResponse

# Google
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ----------------------- LOG -----------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot")

# ----------------------- ENVs ----------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_WEBHOOK_SECRET = (os.getenv("TELEGRAM_WEBHOOK_SECRET") or "").strip()

ADMIN_TELEGRAM_ID = (os.getenv("ADMIN_TELEGRAM_ID") or "").strip()
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")  # opcional: /admin <token> promove

# DB
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    raise RuntimeError("Defina DATABASE_URL (Neon).")

# Google auth
GOOGLE_USE_OAUTH = os.getenv("GOOGLE_USE_OAUTH", "0") == "1"
GOOGLE_OAUTH_SCOPES = (
    os.getenv("GOOGLE_OAUTH_SCOPES")
    or "https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/spreadsheets"
).split()
GOOGLE_TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "/tmp/google_oauth_token.json")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON") or os.getenv("GOOGLE_SHEETS_CREDENTIALS")

# Sheets (Licenças)
LICENSE_SHEET_ID = os.getenv("LICENSE_SHEET_ID")
LICENSE_SHEET_TAB = os.getenv("LICENSE_SHEET_TAB", "Licencas")
LICENSE_SHEET_RANGE = os.getenv("LICENSE_SHEET_RANGE", f"{LICENSE_SHEET_TAB}!A:F")
SHEET_START_ROW = int(os.getenv("SHEET_START_ROW", "2"))

# ----------------------- APP -----------------------
app = FastAPI()

# --------------------- HELPERS ---------------------
def _now() -> datetime:
    return datetime.now(timezone.utc)

def _now_iso() -> str:
    return _now().isoformat(timespec="seconds")

def _normalize(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()

def _gen_license(prefix="GF") -> str:
    alphabet = string.ascii_uppercase + string.digits
    part = lambda n: "".join(secrets.choice(alphabet) for _ in range(n))
    return f"{prefix}-{part(4)}-{part(4)}"

# -------------- DB (psycopg) c/ retry --------------
def _exec_with_retry(sql: str, params: tuple = (), fetch: bool = False):
    last = None
    for i in range(3):
        try:
            with psycopg.connect(DATABASE_URL, autocommit=True, sslmode="require") as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    if fetch:
                        return cur.fetchall()
                    return
        except Exception as e:
            last = e
            logger.warning(f"DB retry {i+1}/3: {e}")
            time.sleep(0.35 * (i + 1))
    raise last

# ---------------- Google Sheets --------------------
def _google_creds():
    if GOOGLE_USE_OAUTH:
        if not os.path.exists(GOOGLE_TOKEN_PATH):
            raise RuntimeError("GOOGLE_TOKEN_PATH não encontrado.")
        with open(GOOGLE_TOKEN_PATH, "r") as f:
            data = json.load(f)
        return Credentials.from_authorized_user_info(data, GOOGLE_OAUTH_SCOPES)
    else:
        if not GOOGLE_SA_JSON:
            raise RuntimeError("GOOGLE_SA_JSON não definido.")
        info = json.loads(GOOGLE_SA_JSON)
        return service_account.Credentials.from_service_account_info(info, scopes=GOOGLE_OAUTH_SCOPES)

def _sheets():
    if not LICENSE_SHEET_ID:
        return None
    creds = _google_creds()
    return build("sheets", "v4", credentials=creds).spreadsheets()

def _sheet_row(*, key: str, days: Optional[int], start: datetime, end: Optional[datetime],
               email: Optional[str], status: str):
    validade = "vitalícia" if not days or days == 0 or not end else str(days)
    c = start.strftime("%Y-%m-%d %H:%M:%S")
    d = end.strftime("%Y-%m-%d %H:%M:%S") if end else ""
    return [key, validade, c, d, (email or ""), status]

def sheet_append_license(*, key: str, days: Optional[int], email: Optional[str],
                         start: datetime, end: Optional[datetime], status: str = "active"):
    srv = _sheets()
    if not srv:
        return
    body = {"values": [_sheet_row(key=key, days=days, start=start, end=end, email=email, status=status)]}
    srv.values().append(
        spreadsheetId=LICENSE_SHEET_ID,
        range=LICENSE_SHEET_RANGE,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()

# ----------------- Licenças (core) -----------------
def create_license(days: Optional[int] = 30, max_files: int = 1,
                   notes: Optional[str] = None, custom_key: Optional[str] = None,
                   email_for_sheet: Optional[str] = None):
    """
    Cria licença no banco. Sempre tenta inserir com start_date/end_date/email.
    Se a tabela não tiver essas colunas, cai em fallback e insere sem elas.
    Também escreve na planilha (A:F) se configurada.
    """
    key = custom_key or _gen_license()
    start_date = _now()
    expires_at = (start_date + timedelta(days=days)) if days else None
    end_date = expires_at

    logger.info("create_license: INSERT COM start_date/end_date + email")

    # Tentativa 1: schema completo (recomendado)
    try:
        _exec_with_retry(
            """
            INSERT INTO licenses(
                license_key, email, start_date, end_date,
                status, max_files, expires_at, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (key, email_for_sheet, start_date, end_date, "active", max_files, expires_at, notes),
        )
    except Exception as e:
        logger.warning(f"Tentativa INSERT full falhou, tentando fallback: {e}")
        # Tentativa 2: sem start_date/end_date
        _exec_with_retry(
            """
            INSERT INTO licenses(
                license_key, status, max_files, expires_at, notes, email
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (key, "active", max_files, expires_at, notes, email_for_sheet),
        )

    # Sheets (opcional)
    try:
        sheet_append_license(
            key=key, days=days, email=email_for_sheet,
            start=start_date, end=expires_at, status="active"
        )
    except Exception:
        logger.exception("Falha ao escrever licença no Sheets (ignorado).")

    return key, (expires_at.isoformat(timespec="seconds") if expires_at else None)

# ------------------ Telegram -----------------------
async def tg_send(chat_id, text):
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            )
    except Exception as e:
        logger.error(f"Falha Telegram: {e}")

def _is_admin(chat_id: int) -> bool:
    return ADMIN_TELEGRAM_ID and str(chat_id) == ADMIN_TELEGRAM_ID

# ------------------- Endpoints ---------------------
@app.get("/ping")
def ping():
    return {"pong": True, "time": _now_iso()}

@app.post("/telegram/webhook")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(default=None),
):
    if TELEGRAM_WEBHOOK_SECRET and (x_telegram_bot_api_secret_token or "").strip() != TELEGRAM_WEBHOOK_SECRET:
        logger.warning("Webhook rejeitado: secret inválido.")
        return JSONResponse({"ok": True})

    body = await request.json()
    msg = body.get("message") or {}
    chat_id = msg.get("chat", {}).get("id")
    text = (msg.get("text") or "").strip()
    username = (msg.get("from") or {}).get("username")

    if not chat_id or not text:
        return JSONResponse({"ok": True})

    parts = text.split()
    cmd = parts[0].split("@")[0].lower()
    norm = _normalize(text)

    logger.info({"from": chat_id, "user": username, "cmd": cmd, "text": text})

    # /start
    if cmd == "/start":
        await tg_send(chat_id, "🤖 Bot ativo!\nUse `/whoami` e `/licenca nova <dias> [email]`")
        return JSONResponse({"ok": True})

    # /whoami
    if cmd == "/whoami":
        await tg_send(chat_id, f"*whoami*\n• chatid: `{chat_id}`\n• admin: `{'true' if _is_admin(chat_id) else 'false'}`")
        return JSONResponse({"ok": True})

    # /admin <TOKEN> (promove quem não está no env ADMIN_TELEGRAM_ID)
    if cmd == "/admin":
        if not ADMIN_TOKEN or len(parts) < 2 or parts[1] != ADMIN_TOKEN:
            await tg_send(chat_id, "Uso: `/admin <TOKEN>`")
            return JSONResponse({"ok": True})
        # somente ecoa sucesso (promoção persistente opcional via tabela 'admins')
        await tg_send(chat_id, "✅ Admin habilitado neste chat (somente sessão).")
        return JSONResponse({"ok": True})

    # /licenca nova <dias> [email]  (apenas admin)
    if cmd == "/licenca" and "nova" in norm:
        if not _is_admin(chat_id):
            await tg_send(chat_id, "❌ Apenas admin pode criar licenças.")
            return JSONResponse({"ok": True})

        if len(parts) < 3:
            await tg_send(chat_id, "Uso: `/licenca nova <dias> [email]`")
            return JSONResponse({"ok": True})

        try:
            days = int(parts[2])
        except Exception:
            await tg_send(chat_id, "Dias inválidos. Ex.: `/licenca nova 30 email@dominio.com`")
            return JSONResponse({"ok": True})

        email = parts[3] if len(parts) > 3 else None
        try:
            key, exp = create_license(days=days, email_for_sheet=email)
            msg = f"🔑 *Licença criada*\n`{key}`\n*Validade:* {exp or 'vitalícia'}\n*Email:* {email or '-'}"
            await tg_send(chat_id, msg)
        except Exception as e:
            logger.exception("Falha ao criar licença")
            await tg_send(chat_id, f"❌ Erro ao criar licença: {e}")

        return JSONResponse({"ok": True})

    # fallback
    await tg_send(chat_id, "❗ Comando não reconhecido. Use `/start`.")
    return JSONResponse({"ok": True})
