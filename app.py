import os
import re
import json
import sqlite3
import secrets
import string
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, Request, Header
from fastapi.responses import HTMLResponse, RedirectResponse

# Google APIs
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ===========================
# Log
# ===========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===========================
# FastAPI
# ===========================
app = FastAPI()

# ===========================
# ENVs
# ===========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()

SQLITE_PATH = os.getenv("SQLITE_PATH", "/tmp/db.sqlite")

# Google Auth
GOOGLE_USE_OAUTH = os.getenv("GOOGLE_USE_OAUTH", "0") == "1"
GOOGLE_OAUTH_CLIENT_ID = os.getenv("GOOGLE_OAUTH_CLIENT_ID")
GOOGLE_OAUTH_CLIENT_SECRET = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET")
GOOGLE_OAUTH_REDIRECT_URI = os.getenv("GOOGLE_OAUTH_REDIRECT_URI")
GOOGLE_OAUTH_SCOPES = (
    os.getenv("GOOGLE_OAUTH_SCOPES")
    or "https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/spreadsheets"
).split()
GOOGLE_TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "/tmp/google_oauth_token.json")
OAUTH_STATE_SECRET = os.getenv("OAUTH_STATE_SECRET", "change-me")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")

GS_TEMPLATE_ID = os.getenv("GS_TEMPLATE_ID")
GS_DEST_FOLDER_ID = os.getenv("GS_DEST_FOLDER_ID")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "🧾")
SHEET_FIRST_COL = os.getenv("SHEET_FIRST_COL", "B")
SHEET_LAST_COL = os.getenv("SHEET_LAST_COL", "I")
SHEET_START_ROW = int(os.getenv("SHEET_START_ROW", "8"))
SHARE_LINK_ROLE = os.getenv("SHARE_LINK_ROLE", "writer")

SCOPES_SA = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

LICENSE_SHEET_ID = os.getenv("LICENSE_SHEET_ID")
LICENSE_SHEET_TAB = os.getenv("LICENSE_SHEET_TAB", "Licencas")

APP_TZ = os.getenv("APP_TZ", "America/Sao_Paulo")

# ===========================
# DB
# ===========================
def _db():
    return sqlite3.connect(SQLITE_PATH)

def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _local_today():
    try:
        return datetime.now(ZoneInfo(APP_TZ)).date()
    except Exception:
        return datetime.now().date()
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
    cur.execute("""
    CREATE TABLE IF NOT EXISTS usage (
        chat_id TEXT,
        event TEXT,
        ts TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pending (
        chat_id TEXT PRIMARY KEY,
        step TEXT,
        temp_license TEXT,
        created_at TEXT
    )""")
    con.commit()
    con.close()

def record_usage(chat_id, event):
    con = _db()
    con.execute("INSERT INTO usage(chat_id, event, ts) VALUES(?,?,?)",
                (str(chat_id), event, _now_iso()))
    con.commit()
    con.close()

def _gen_key(prefix="GF"):
    alphabet = string.ascii_uppercase + string.digits
    part = lambda n: "".join(secrets.choice(alphabet) for _ in range(n))
    return f"{prefix}-{part(4)}-{part(4)}"

# ===== Pending (licença/email)
def set_pending(chat_id: str, step: Optional[str], temp_license: Optional[str]):
    con = _db()
    con.execute("""
        CREATE TABLE IF NOT EXISTS pending (
            chat_id TEXT PRIMARY KEY,
            step TEXT,
            temp_license TEXT,
            created_at TEXT
        )
    """)
    if step:
        con.execute("""
            INSERT INTO pending(chat_id, step, temp_license, created_at)
            VALUES(?,?,?,?)
            ON CONFLICT(chat_id) DO UPDATE SET step=excluded.step, temp_license=excluded.temp_license, created_at=excluded.created_at
        """, (str(chat_id), step, temp_license, _now_iso()))
    else:
        con.execute("DELETE FROM pending WHERE chat_id=?", (str(chat_id),))
    con.commit()
    con.close()

def get_pending(chat_id: str) -> tuple[Optional[str], Optional[str]]:
    con = _db()
    cur = con.execute("SELECT step, temp_license FROM pending WHERE chat_id=?", (str(chat_id),))
    row = cur.fetchone()
    con.close()
    if not row:
        return None, None
    return row[0], row[1]

# ===========================
# Telegram helpers
# ===========================
async def tg_send(chat_id, text):
    async with httpx.AsyncClient(timeout=12) as client:
        try:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            )
        except Exception as e:
            logger.error(f"Erro ao enviar msg: {e}")

async def tg_send_with_kb(chat_id, text, keyboard):
    async with httpx.AsyncClient(timeout=12) as client:
        try:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "Markdown",
                    "reply_markup": {"inline_keyboard": keyboard},
                },
            )
        except Exception as e:
            logger.error(f"Erro ao enviar msg com teclado: {e}")

# ===========================
# Botões de grupo (inline keyboard)
# ===========================
GROUP_CHOICES = [
    ("💸Gastos Variáveis", "GASTOS_VARIAVEIS"),
    ("🏠Gastos Fixos", "GASTOS_FIXOS"),
    ("📺Assinatura", "ASSINATURA"),
    ("🧾Despesas Temporárias", "DESPESAS_TEMP"),
    ("💳Pagamento de Fatura", "PAG_FATURA"),
    ("💵Ganhos", "GANHOS"),
    ("💰Investimento", "INVESTIMENTO"),
    ("📝Reserva", "RESERVA"),
    ("💲Saque/Resgate", "SAQUE_RESGATE"),
]

def _group_label_by_key(k: str) -> str:
    for lbl, key in GROUP_CHOICES:
        if key == k:
            return lbl
    return "💸Gastos Variáveis"

def _group_keyboard_rows():
    rows = []
    row = []
    for i, (label, key) in enumerate(GROUP_CHOICES, 1):
        row.append({"text": label, "callback_data": f"grp:{key}"})
        if i % 3 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows
GROUP_EXAMPLE = {
    "GASTOS_VARIAVEIS": "Mercado, 59,90 no débito hoje",
    "GASTOS_FIXOS": "Aluguel, 2800 via Pix hoje",
    "INVESTIMENTO": "Renda fixa, 1000 via Pix hoje",
    "ASSINATURA": "Netflix, 49 no cartão Santander hoje",
    "DESPESAS_TEMP": "IPTU, 120 no débito hoje",
    "GANHOS": "Salário, 4500 via Pix hoje",
    "RESERVA": "Viagem pra Europa, 500 via Pix hoje",
    "SAQUE_RESGATE": "Renda variável, 400 via Pix hoje",
    "PAG_FATURA": "Cartão Nubank, 3300 via Pix hoje",
}

def detect_installments(text: str) -> str:
    t = text.lower()

    # Detectar formato de parcelamento
    m = re.search(r"(\d+)\s*x", t)
    if m:
        parcelas = m.group(1)
        parcelas = parcelas.strip()
        return f"{parcelas}x"

    # Detectar "à vista"
    if "à vista" in t or "a vista" in t:
        return "à vista"

    # Caso não haja referência
    return "1x"

def detect_payment(text: str) -> str:
    t = text.lower()
    m = re.search(r"cart[aã]o\s+([a-z0-9 ]+)", t)
    if m:
        brand = re.sub(r"\s+", " ", m.group(1)).strip()
        brand = _clean_trailing_tokens(brand)
        if brand:
            return f"cartão {_titlecase(brand)}"
        return "cartão"
    if "pix" in t:
        return "Pix"
    if "débito" in t or "debito" in t:
        return "débito"
    if "crédito" in t or "credito" in t:
        return "crédito"
    return "Outros"

# ===========================
# NLP (modo texto livre)
# ===========================
def parse_natural(text: str) -> Tuple[Optional[List], Optional[str]]:
    valor = parse_money(text)
    if valor is None:
        return None, "Não achei o valor. Ex.: 45,90"

    data_br = parse_date(text) or _local_today().strftime("%d/%m/%Y")
    forma = detect_payment(text)
    cond = detect_installments(text)

    group_label, category = detect_group_and_category_free(text)

    # Pagamento de fatura → forma nunca é "cartão ..."
    if group_label == GROUP_EMOJI["PAG_FATURA"] and str(forma).startswith("cartão"):
        t_low = text.lower()
        if "pix" in t_low:
            forma = "Pix"
        elif ("débito" in t_low) or ("debito" in t_low):
            forma = "débito"
        else:
            forma = "Outros"

    # Tipo por grupo
    if group_label == GROUP_EMOJI["INVESTIMENTO"]:
        tipo = "▼ Saída"
    elif group_label in (GROUP_EMOJI["SAQUE_RESGATE"], GROUP_EMOJI["GANHOS"]):
        tipo = "▲ Entrada"
    elif group_label == GROUP_EMOJI["PAG_FATURA"]:
        tipo = "▼ Saída"
    else:
        tipo = "▼ Saída"

    desc = ""  # sempre vazio

    return [data_br, tipo, group_label, category, desc, float(valor), forma, cond], None
