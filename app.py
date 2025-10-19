# app.py — Telegram + FastAPI + Neon (Postgres) + (opcional) Google Sheets
# Resiliente a nomes de colunas com acentos/hífens/variações: mapeia dinamicamente pelo information_schema.

import os
import json
import time
import re
import unicodedata
import random
import string
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple

import psycopg
import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# ========================= LOG =========================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

# ========================= ENVs ========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
ADMIN_TELEGRAM_ID = (os.getenv("ADMIN_TELEGRAM_ID") or "").strip()
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")  # opcional: /admin <TOKEN>

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    raise RuntimeError("Defina DATABASE_URL.")

# Google Sheets (opcional)
LICENSE_SHEET_ID    = os.getenv("LICENSE_SHEET_ID")
LICENSE_SHEET_TAB   = os.getenv("LICENSE_SHEET_TAB", "Licencas")
LICENSE_SHEET_RANGE = os.getenv("LICENSE_SHEET_RANGE", f"{LICENSE_SHEET_TAB}!A:F")
GOOGLE_USE_OAUTH  = os.getenv("GOOGLE_USE_OAUTH", "0") == "1"
GOOGLE_TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "/tmp/google_oauth_token.json")
GOOGLE_SA_JSON    = os.getenv("GOOGLE_SA_JSON") or os.getenv("GOOGLE_SHEETS_CREDENTIALS")
GOOGLE_SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]

# ========================= APP =========================
app = FastAPI()

# ======================== UTILS ========================
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _gen_key(prefix="GF") -> str:
    chars = string.ascii_uppercase + string.digits
    return f"{prefix}-{''.join(random.choices(chars, k=4))}-{''.join(random.choices(chars, k=4))}"

def tg_send(chat_id: int, text: str):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": chat_id, "text": text})
    except Exception as e:
        log.warning(f"Falha Telegram: {e}")

def _exec_with_retry(sql: str, params: tuple = (), fetch: bool = False):
    last = None
    for i in range(3):
        try:
            with psycopg.connect(DATABASE_URL, sslmode="require", autocommit=True) as con:
                with con.cursor() as cur:
                    cur.execute(sql, params)
                    if fetch:
                        return cur.fetchall()
            return
        except Exception as e:
            last = e
            log.warning(f"DB retry {i+1}/3: {e}")
            time.sleep(0.35 * (i + 1))
    raise last

# -------- normalização robusta de identificadores -------
_id_clean_re = re.compile(r"[^a-z0-9]+")
def _norm_ident(s: str) -> str:
    # remove diacríticos, lower, troca tudo que não é [a-z0-9] por _
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = _id_clean_re.sub("_", s)
    return s.strip("_")

# -------- mapeamento dinâmico dos nomes reais -----------
_COLMAP_CACHE: Dict[str, str] = {}

# chaves canônicas que queremos (sem acento)
CANONICAL_KEYS = {
    "license_key": ["chave_de_licenca", "licenca", "license_key", "chave"],
    "status": ["status"],
    "max_files": ["arquivos_max", "max_files", "arquivos", "maximos"],
    "expires_at": ["expira_em", "expires_at", "data_expira"],
    "notes": ["notas", "notes", "observacoes", "obs"],
    "email": ["e_mail", "email"],
    "start_date": ["data_de_inicio", "inicio", "start_date"],
    "end_date": ["data_final", "fim", "end_date"],
    "days": ["dias_de_validade", "dias", "validade_dias"],
}

def _load_column_map() -> Dict[str, str]:
    """
    Retorna um dicionário: {canonical_key -> "NomeRealComAspas"}
    usando information_schema.columns, tolerando acentos/hífens/espaços.
    """
    global _COLMAP_CACHE
    if _COLMAP_CACHE:
        return _COLMAP_CACHE

    rows = _exec_with_retry(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'licenses'
        ORDER BY ordinal_position
        """,
        fetch=True
    ) or []

    # prepara listas normalizadas
    norm_to_real: Dict[str, str] = {}
    for (col_name,) in rows:
        norm = _norm_ident(col_name)
        norm_to_real[norm] = col_name  # salva nome EXATO

    result: Dict[str, str] = {}
    for canon, aliases in CANONICAL_KEYS.items():
        found = None
        for alias in aliases:
            if alias in norm_to_real:
                found = norm_to_real[alias]
                break
        # heurística extra: tentar variantes sem underscores
        if not found:
            for alias in aliases:
                alias_no = alias.replace("_", "")
                for norm, real in norm_to_real.items():
                    if norm.replace("_", "") == alias_no:
                        found = real
                        break
                if found:
                    break
        if found:
            result[canon] = f"\"{found}\""  # sempre cita com aspas
        else:
            result[canon] = ""  # não encontrado

    _COLMAP_CACHE = result
    log.info({"colmap": _COLMAP_CACHE})
    return result

# -------------- Google Sheets (opcional) --------------
_sheets = None
def _sheets_client():
    global _sheets
    if _sheets or not LICENSE_SHEET_ID:
        return _sheets
    try:
        if GOOGLE_USE_OAUTH:
            from google.oauth2.credentials import Credentials
            if not os.path.exists(GOOGLE_TOKEN_PATH):
                raise RuntimeError("OAuth token não encontrado.")
            with open(GOOGLE_TOKEN_PATH, "r") as f:
                data = json.load(f)
            creds = Credentials.from_authorized_user_info(data, GOOGLE_SCOPES)
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

def _sheet_row(key: str, days: Optional[int], start_dt: datetime,
               end_dt: Optional[datetime], email: Optional[str], status: str):
    validade = "vitalícia" if not days or days == 0 or not end_dt else str(days)
    inicio = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    fim = end_dt.strftime("%Y-%m-%d %H:%M:%S") if end_dt else ""
    return [key, validade, inicio, fim, (email or ""), status]

def sheet_append_license(key: str, days: Optional[int], email: Optional[str],
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
            body={"values": [_sheet_row(key, days, start_dt, end_dt, email, status)]},
        ).execute()
    except Exception as e:
        log.warning(f"Sheets append falhou: {e}")

# ================== CORE: LICENÇAS ====================
def create_license(days: int = 30, max_files: int = 1,
                   notes: Optional[str] = None, custom_key: Optional[str] = None,
                   email_for_sheet: Optional[str] = None):
    """
    Cria licença resolvendo dinamicamente os nomes reais das colunas da tabela 'licenses'.
    Evita erros de 'column does not exist' causados por acentos/hífens/variações.
    """
    key = custom_key or _gen_key()
    start_dt = _now_utc()
    expires_at = start_dt + timedelta(days=days) if days else None
    end_dt = expires_at
    status_val = "active"

    col = _load_column_map()

    # constrói lista de colunas/valores presentes no seu schema
    cols: List[str] = []
    vals: List = []

    # ordem não importa, mas tentamos manter lógico
    for canon, value in [
        ("license_key", key),
        ("status", status_val),
        ("max_files", max_files),
        ("expires_at", expires_at),
        ("notes", notes),
        ("email", email_for_sheet),
        ("start_date", start_dt),
        ("end_date", end_dt),
        ("days", days),
    ]:
        real = col.get(canon) or ""
        if real:
            cols.append(real)
            vals.append(value)

    if not (col.get("license_key") and col.get("status")):
        raise RuntimeError("Não encontrei colunas essenciais (license_key/status) na tabela 'licenses'.")

    placeholders = ",".join(["%s"] * len(cols))
    cols_sql = ", ".join(cols)
    sql = f"INSERT INTO licenses ({cols_sql}) VALUES ({placeholders})"

    log.info({"insert_sql": sql, "values_preview": str(vals)[:120]})
    _exec_with_retry(sql, tuple(vals))

    try:
        sheet_append_license(key, days, email_for_sheet, start_dt, end_dt, status_val)
    except Exception:
        log.exception("Falha ao escrever no Sheets (ignorado).")

    return key, (expires_at.strftime("%Y-%m-%d %H:%M:%S") if expires_at else "vitalícia")

# ================== TELEGRAM BOT ======================
def _is_admin(chat_id: int) -> bool:
    return ADMIN_TELEGRAM_ID and str(chat_id) == ADMIN_TELEGRAM_ID

@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    body = await request.json()
    msg = body.get("message") or {}
    chat_id = msg.get("chat", {}).get("id")
    text = (msg.get("text") or "").strip()

    if not chat_id or not text:
        return {"ok": True}

    log.info({"from": chat_id, "text": text})

    if text.startswith("/start"):
        tg_send(chat_id, "🤖 Bot ativo!\nUse: /whoami\nUse: /licenca nova <dias> [email]")
        return {"ok": True}

    if text.startswith("/whoami"):
        tg_send(chat_id, f"• chatid: {chat_id}\n• admin: {'true' if _is_admin(chat_id) else 'false'}")
        return {"ok": True}

    if text.startswith("/admin"):
        parts = text.split(maxsplit=1)
        if len(parts) == 2 and ADMIN_TOKEN and parts[1].strip() == ADMIN_TOKEN:
            global ADMIN_TELEGRAM_ID
            ADMIN_TELEGRAM_ID = str(chat_id)
            tg_send(chat_id, "✅ Admin habilitado neste chat.")
        else:
            tg_send(chat_id, "❌ Token inválido. Uso: /admin <TOKEN>")
        return {"ok": True}

    if text.lower().startswith("/licenca nova"):
        if not _is_admin(chat_id):
            tg_send(chat_id, "❌ Apenas admin pode criar licenças.")
            return {"ok": True}

        parts = text.split()
        if len(parts) < 3:
            tg_send(chat_id, "❌ Uso: /licenca nova <dias> [email]")
            return {"ok": True}

        try:
            days = int(parts[2])
        except Exception:
            tg_send(chat_id, "Dias inválidos. Ex.: /licenca nova 30 email@dominio.com")
            return {"ok": True}

        email = parts[3] if len(parts) > 3 else None
        try:
            key, exp = create_license(days=days, email_for_sheet=email)
            tg_send(chat_id, f"✅ Licença criada!\n🔑 {key}\n📅 Validade: {exp}\n📧 {email or '(sem email)'}")
        except Exception as e:
            log.exception("Erro ao criar licença")
            tg_send(chat_id, f"❌ Erro ao criar licença: {e}")
        return {"ok": True}

    tg_send(chat_id, "❗ Comando não reconhecido. Use /start")
    return {"ok": True}

# ================== Healthcheck =======================
@app.get("/ping")
def ping():
    return {"pong": True, "time": datetime.now(timezone.utc).isoformat(timespec="seconds")}
