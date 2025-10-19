# app.py — Bot Telegram + FastAPI + Neon (Postgres) + (opcional) Google Sheets
# Compatível com os nomes de coluna atuais no seu banco (PT-BR com acento/espaço/hífen).

import os
import json
import time
import secrets
import string
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from fastapi import FastAPI, Request, Header
from fastapi.responses import JSONResponse

import psycopg
from psycopg import sql

# ============ LOG ============
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

# ============ ENVs ============
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    raise RuntimeError("Defina DATABASE_URL.")

ADMIN_TELEGRAM_ID = (os.getenv("ADMIN_TELEGRAM_ID") or "").strip()
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")  # opcional: /admin <TOKEN>
TELEGRAM_WEBHOOK_SECRET = (os.getenv("TELEGRAM_WEBHOOK_SECRET") or "").strip()

# Google Sheets (opcional)
LICENSE_SHEET_ID    = os.getenv("LICENSE_SHEET_ID")
LICENSE_SHEET_TAB   = os.getenv("LICENSE_SHEET_TAB", "Licencas")
LICENSE_SHEET_RANGE = os.getenv("LICENSE_SHEET_RANGE", f"{LICENSE_SHEET_TAB}!A:F")
GOOGLE_USE_OAUTH    = os.getenv("GOOGLE_USE_OAUTH", "0") == "1"
GOOGLE_TOKEN_PATH   = os.getenv("GOOGLE_TOKEN_PATH", "/tmp/google_oauth_token.json")
GOOGLE_SA_JSON      = os.getenv("GOOGLE_SA_JSON") or os.getenv("GOOGLE_SHEETS_CREDENTIALS")
GOOGLE_SCOPES       = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# ============ APP ============
app = FastAPI()

# ============ COLUNAS (NOMES EXATOS DO SEU BANCO) ============
COL = {
    "id":            'eu ia',
    "license_key":   'chave_de_licença',
    "email":         'e-mail',
    "start_date":    'data_de_início',
    "end_date":      'data_final',
    "validity_days": 'dias_de_validade',
    "max_files":     'arquivos_máx.',
    "expires_at":    'expira_em',
    "notes":         'notas',
    "status":        'status',
}

# ============ UTILS ============
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _gen_license(prefix="GF") -> str:
    alphabet = string.ascii_uppercase + string.digits
    def part(n): return "".join(secrets.choice(alphabet) for _ in range(n))
    return f"{prefix}-{part(4)}-{part(4)}"

def tg_send(chat_id: int, text: str):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": chat_id, "text": text})
    except Exception as e:
        log.warning(f"Falha Telegram: {e}")

def _exec(sql_text: str, params: tuple = (), fetch: bool = False):
    with psycopg.connect(DATABASE_URL, sslmode="require", autocommit=True) as con:
        with con.cursor() as cur:
            cur.execute(sql_text, params)
            if fetch:
                return cur.fetchall()

def _exec_sql(sql_obj, params: tuple = (), fetch: bool = False):
    # versão que aceita psycopg.sql.SQL/Identifier (para nomes com acento/hífen)
    with psycopg.connect(DATABASE_URL, sslmode="require", autocommit=True) as con:
        with con.cursor() as cur:
            cur.execute(sql_obj, params)
            if fetch:
                return cur.fetchall()

def _is_admin(chat_id: int) -> bool:
    return ADMIN_TELEGRAM_ID and str(chat_id) == ADMIN_TELEGRAM_ID

# ============ Google Sheets (opcional) ============
_sheets = None
def _sheets_client():
    global _sheets
    if _sheets or not LICENSE_SHEET_ID:
        return _sheets
    try:
        if GOOGLE_USE_OAUTH:
            from google.oauth2.credentials import Credentials
            if not os.path.exists(GOOGLE_TOKEN_PATH):
                raise RuntimeError("OAuth token não encontrado (GOOGLE_TOKEN_PATH).")
            with open(GOOGLE_TOKEN_PATH, "r") as f:
                creds = Credentials.from_authorized_user_info(json.load(f), GOOGLE_SCOPES)
        else:
            from google.oauth2.service_account import Credentials as SACreds
            if not GOOGLE_SA_JSON:
                raise RuntimeError("GOOGLE_SA_JSON ausente.")
            info = json.loads(GOOGLE_SA_JSON)
            creds = SACreds.from_service_account_info(info, scopes=GOOGLE_SCOPES)
        from googleapiclient.discovery import build
        _sheets = build("sheets", "v4", credentials=creds).spreadsheets()
    except Exception as e:
        log.warning(f"Sheets desativado: {e}")
        _sheets = None
    return _sheets

def _sheet_row(licenca: str, validade_dias: Optional[int], start_dt: datetime,
               end_dt: Optional[datetime], email: Optional[str], status: str):
    validade_cell = "vitalícia" if not validade_dias or validade_dias == 0 or not end_dt else str(validade_dias)
    inicio = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    fim = end_dt.strftime("%Y-%m-%d %H:%M:%S") if end_dt else ""
    return [licenca, validade_cell, inicio, fim, (email or ""), status]

def sheet_append_license(licenca: str, validade_dias: Optional[int], email: Optional[str],
                         start_dt: datetime, end_dt: Optional[datetime], status: str = "active"):
    sc = _sheets_client()
    if not sc:
        return
    try:
        sc.values().append(
            spreadsheetId=LICENSE_SHEET_ID,
            range=LICENSE_SHEET_RANGE,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [_sheet_row(licenca, validade_dias, start_dt, end_dt, email, status)]},
        ).execute()
    except Exception as e:
        log.warning(f"Sheets append falhou: {e}")

# ============ CORE: LICENÇAS ============
def create_license(days: int = 30, email: Optional[str] = None,
                   notes: Optional[str] = None, max_files: int = 1, status: str = "active",
                   custom_key: Optional[str] = None):
    """
    Cria licença usando os NOMES EXATOS da sua tabela.
    Preenche sempre: chave_de_licença, e-mail, data_de_início, data_final, dias_de_validade,
    arquivos_máx., expira_em, notas, status.
    """
    key = custom_key or _gen_license()
    start_dt = _now_utc()
    end_dt = (start_dt + timedelta(days=days)) if days and days > 0 else None
    # expira_em no final do dia de end_dt (se houver)
    expires_at = end_dt.replace(hour=23, minute=59, second=59, microsecond=0) if end_dt else None

    log.info("create_license: INSERT usando colunas PT-BR com aspas/acentos")

    q = sql.SQL("""
        INSERT INTO licenses (
            {license_key},
            {email},
            {start_date},
            {end_date},
            {validity_days},
            {max_files},
            {expires_at},
            {notes},
            {status}
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING {license_key}, {start_date}, {end_date}, {validity_days}, {status}
    """).format(
        license_key=sql.Identifier(COL["license_key"]),
        email=sql.Identifier(COL["email"]),
        start_date=sql.Identifier(COL["start_date"]),
        end_date=sql.Identifier(COL["end_date"]),
        validity_days=sql.Identifier(COL["validity_days"]),
        max_files=sql.Identifier(COL["max_files"]),
        expires_at=sql.Identifier(COL["expires_at"]),
        notes=sql.Identifier(COL["notes"]),
        status=sql.Identifier(COL["status"]),
    )

    row = _exec_sql(q, (
        key,
        email,
        start_dt,
        end_dt,
        days,
        max_files,
        expires_at,
        notes,
        status,
    ), fetch=True)

    # Sheets (opcional)
    try:
        sheet_append_license(key, days, email, start_dt, end_dt, status)
    except Exception:
        log.exception("Falha ao escrever no Sheets (ignorado).")

    return key, (end_dt.strftime("%Y-%m-%d %H:%M:%S") if end_dt else "vitalícia")

def get_license_by_key(license_key: str):
    q = sql.SQL("""
        SELECT
            {license_key}, {email}, {start_date}, {end_date},
            {validity_days}, {max_files}, {expires_at}, {notes}, {status}
        FROM licenses
        WHERE {license_key} = %s
        LIMIT 1
    """).format(
        license_key=sql.Identifier(COL["license_key"]),
        email=sql.Identifier(COL["email"]),
        start_date=sql.Identifier(COL["start_date"]),
        end_date=sql.Identifier(COL["end_date"]),
        validity_days=sql.Identifier(COL["validity_days"]),
        max_files=sql.Identifier(COL["max_files"]),
        expires_at=sql.Identifier(COL["expires_at"]),
        notes=sql.Identifier(COL["notes"]),
        status=sql.Identifier(COL["status"]),
    )
    rows = _exec_sql(q, (license_key,), fetch=True)
    if not rows:
        return None
    r = rows[0]
    return {
        "license_key": r[0],
        "email": r[1],
        "start_date": r[2],
        "end_date": r[3],
        "validity_days": r[4],
        "max_files": r[5],
        "expires_at": r[6],
        "notes": r[7],
        "status": r[8],
    }

# ============ TELEGRAM ============
@app.post("/telegram/webhook")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(default=None),
):
    # valida secret do webhook (se configurado)
    if TELEGRAM_WEBHOOK_SECRET:
        got = (x_telegram_bot_api_secret_token or "").strip()
        if got != TELEGRAM_WEBHOOK_SECRET:
            log.warning("Webhook rejeitado: secret inválido.")
            return JSONResponse({"ok": True})

    body = await request.json()
    msg = body.get("message") or {}
    chat_id = msg.get("chat", {}).get("id")
    text = (msg.get("text") or "").strip()

    if not chat_id or not text:
        return JSONResponse({"ok": True})

    log.info({"from": chat_id, "text": text})

    if text.startswith("/start"):
        tg_send(chat_id, "🤖 Bot ativo!\nComandos:\n/whoami\n/licenca nova <dias> [email]")
        return JSONResponse({"ok": True})

    if text.startswith("/whoami"):
        tg_send(chat_id, f"• chatid: {chat_id}\n• admin: {'true' if _is_admin(chat_id) else 'false'}")
        return JSONResponse({"ok": True})

    if text.startswith("/admin"):
        parts = text.split(maxsplit=1)
        if len(parts) == 2 and ADMIN_TOKEN and parts[1].strip() == ADMIN_TOKEN:
            global ADMIN_TELEGRAM_ID
            ADMIN_TELEGRAM_ID = str(chat_id)
            tg_send(chat_id, "✅ Admin habilitado neste chat.")
        else:
            tg_send(chat_id, "❌ Token inválido. Uso: /admin <TOKEN>")
        return JSONResponse({"ok": True})

    if text.lower().startswith("/licenca nova"):
        if not _is_admin(chat_id):
            tg_send(chat_id, "❌ Apenas admin pode criar licenças.")
            return JSONResponse({"ok": True})

        parts = text.split()
        if len(parts) < 3:
            tg_send(chat_id, "❌ Uso: /licenca nova <dias> [email]")
            return JSONResponse({"ok": True})

        try:
            days = int(parts[2])
        except Exception:
            tg_send(chat_id, "Dias inválidos. Ex.: /licenca nova 30 email@dominio.com")
            return JSONResponse({"ok": True})

        email = parts[3] if len(parts) > 3 else None
        try:
            key, exp = create_license(days=days, email=email)
            tg_send(chat_id, f"✅ Licença criada!\n🔑 {key}\n📅 Validade: {exp}\n📧 {email or '(sem email)'}")
        except Exception as e:
            log.exception("Erro ao criar licença")
            tg_send(chat_id, f"❌ Erro ao criar licença: {e}")
        return JSONResponse({"ok": True})

    tg_send(chat_id, "❗ Comando não reconhecido. Use /start")
    return JSONResponse({"ok": True})

# ============ Health ============
@app.get("/ping")
def ping():
    return {"pong": True, "time": datetime.now(timezone.utc).isoformat(timespec="seconds")}
