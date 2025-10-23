import os
import re
import json
import sqlite3
import secrets
import string
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List

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
    os.getenv("GOOGLE_OAUTH_SCOPES") or
    "https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/spreadsheets"
).split()
GOOGLE_TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "/tmp/google_oauth_token.json")
OAUTH_STATE_SECRET = os.getenv("OAUTH_STATE_SECRET", "change-me")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")

# IDs do modelo e pasta (Drive)
GS_TEMPLATE_ID = os.getenv("GS_TEMPLATE_ID")
GS_DEST_FOLDER_ID = os.getenv("GS_DEST_FOLDER_ID")

# Planilha de lançamentos
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "🧾")
SHEET_FIRST_COL = os.getenv("SHEET_FIRST_COL", "B")
SHEET_LAST_COL = os.getenv("SHEET_LAST_COL", "I")
SHEET_START_ROW = int(os.getenv("SHEET_START_ROW", "8"))

SHARE_LINK_ROLE = os.getenv("SHARE_LINK_ROLE", "writer")

SCOPES_SA = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# Planilha de Licenças
LICENSE_SHEET_ID = os.getenv("LICENSE_SHEET_ID")
LICENSE_SHEET_TAB = os.getenv("LICENSE_SHEET_TAB", "Licencas")

# ===========================
# OAuth helper + routes
# ===========================
def _client_config_dict():
    return {
        "web": {
            "client_id": GOOGLE_OAUTH_CLIENT_ID,
            "client_secret": GOOGLE_OAUTH_CLIENT_SECRET,
            "redirect_uris": [GOOGLE_OAUTH_REDIRECT_URI],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    }

def _save_credentials(creds: Credentials):
    data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
    }
    os.makedirs(os.path.dirname(GOOGLE_TOKEN_PATH), exist_ok=True)
    with open(GOOGLE_TOKEN_PATH, "w") as f:
        json.dump(data, f)

@app.get("/oauth/start")
def oauth_start():
    if not GOOGLE_USE_OAUTH:
        return HTMLResponse("<h3>OAuth desabilitado. Defina GOOGLE_USE_OAUTH=1.</h3>", status_code=400)
    if not (GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET and GOOGLE_OAUTH_REDIRECT_URI):
        return HTMLResponse("<h3>Faltam variáveis do OAuth no ambiente.</h3>", status_code=500)

    flow = Flow.from_client_config(
        _client_config_dict(),
        scopes=GOOGLE_OAUTH_SCOPES,
        redirect_uri=GOOGLE_OAUTH_REDIRECT_URI
    )
    auth_url, _state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
        state=OAUTH_STATE_SECRET,
    )
    return RedirectResponse(auth_url)

@app.get("/oauth/callback")
def oauth_callback(code: str | None = None, state: str | None = None):
    if not GOOGLE_USE_OAUTH:
        return HTMLResponse("<h3>OAuth desabilitado. Defina GOOGLE_USE_OAUTH=1.</h3>", status_code=400)
    if state != OAUTH_STATE_SECRET:
        return HTMLResponse("<h3>State inválido.</h3>", status_code=400)
    if not code:
        return HTMLResponse("<h3>Faltou 'code' na URL.</h3>", status_code=400)

    flow = Flow.from_client_config(
        _client_config_dict(),
        scopes=GOOGLE_OAUTH_SCOPES,
        redirect_uri=GOOGLE_OAUTH_REDIRECT_URI
    )
    flow.fetch_token(code=code)
    creds = flow.credentials
    if not creds.refresh_token:
        return HTMLResponse("<h3>Não veio refresh_token. Refazer /oauth/start.</h3>", status_code=400)

    _save_credentials(creds)
    return HTMLResponse("<h3>✅ OAuth ok! Pode voltar ao Telegram.</h3>")
# ===========================
# DB
# ===========================
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

# Pendências (licença/email)
def set_pending(chat_id: str, step: Optional[str], temp_license: Optional[str]):
    con = _db()
    if step:
        con.execute("""
            INSERT INTO pending(chat_id, step, temp_license, created_at)
            VALUES(?,?,?,?)
            ON CONFLICT(chat_id) DO UPDATE SET
                step=excluded.step,
                temp_license=excluded.temp_license,
                created_at=excluded.created_at
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

# Exemplos dinâmicos por grupo (usados ao clicar no botão)
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

# ===========================
# Estado "grupo selecionado"
# ===========================
def _ensure_group_state_table():
    con = _db()
    con.execute("""
        CREATE TABLE IF NOT EXISTS pending_group (
            chat_id TEXT PRIMARY KEY,
            group_key TEXT,
            updated_at TEXT
        )
    """)
    con.commit()
    con.close()

def set_selected_group(chat_id: str, group_key: Optional[str]):
    _ensure_group_state_table()
    con = _db()
    if group_key is None:
        con.execute("DELETE FROM pending_group WHERE chat_id=?", (str(chat_id),))
    else:
        con.execute("""
            INSERT INTO pending_group(chat_id, group_key, updated_at)
            VALUES(?,?,?)
            ON CONFLICT(chat_id) DO UPDATE SET group_key=excluded.group_key, updated_at=excluded.updated_at
        """, (str(chat_id), group_key, _now_iso()))
    con.commit()
    con.close()

def get_selected_group(chat_id: str) -> Optional[str]:
    _ensure_group_state_table()
    con = _db()
    cur = con.execute("SELECT group_key FROM pending_group WHERE chat_id=?", (str(chat_id),))
    row = cur.fetchone()
    con.close()
    return row[0] if row else None
# ===========================
# Parsing helpers
# ===========================
from zoneinfo import ZoneInfo
APP_TZ = os.getenv("APP_TZ", "America/Sao_Paulo")

def _local_today():
    try:
        return datetime.now(ZoneInfo(APP_TZ)).date()
    except Exception:
        return datetime.now().date()

def _titlecase(s: str) -> str:
    return " ".join(w.capitalize() for w in s.split())

TRAILING_STOP = {
    "hoje","ontem","amanha","amanhã","agora","hj",
    "via","no","na","em","de","do","da","e",
    "pix","débito","debito","crédito","credito","valor"
}

def _clean_trailing_tokens(s: str) -> str:
    tokens = s.split()
    while tokens and tokens[-1].lower() in TRAILING_STOP:
        tokens.pop()
    return " ".join(tokens).strip()

def _format_date_br(d: datetime.date) -> str:
    return d.strftime("%d/%m/%Y")

def parse_date(text: str) -> Optional[str]:
    t = text.lower()
    today = _local_today()
    if "hoje" in t:
        return _format_date_br(today)
    if "ontem" in t:
        return _format_date_br(today - timedelta(days=1))
    m = re.search(r"\b(\d{1,2})[\/\-.](\d{1,2})(?:[\/\-.](\d{2,4}))?\b", t)
    if m:
        d = int(m.group(1)); mo = int(m.group(2))
        y = int(m.group(3)) if m.group(3) else today.year
        if y < 100: y += 2000
        try:
            return _format_date_br(datetime(y, mo, d).date())
        except:
            return None
    return None

def parse_money(text: str) -> Optional[float]:
    t = text.lower().replace("r$", " ").replace("reais", " ")
    t = re.sub(r"\b\d{1,2}[\/\-.]\d{1,2}(?:[\/\-.]\d{2,4})?\b", " ", t)
    matches = re.findall(
        r"\b\d{1,3}(?:[.\s]\d{3})*(?:,\d{1,2})\b|\b\d+(?:[.,]\d{1,2})\b|\b\d+\b",
        t
    )
    if not matches:
        return None
    raw = matches[-1].replace(" ", "")
    if "," in raw and "." in raw:
        raw = raw.replace(".", "").replace(",", ".")
    else:
        raw = raw.replace(",", ".")
    try:
        return float(raw)
    except:
        return None

def detect_payment(text: str) -> str:
    t = text.lower()
    m = re.search(r"cart[aã]o\s+([a-z0-9 ]+)", t)
    if m:
        brand = re.sub(r"\s+", " ", m.group(1)).strip()
        brand = _clean_trailing_tokens(brand)
        if brand:
            return f"💳cartão {_titlecase(brand)}"
        return "💳cartão"
    if "pix" in t: return "Pix"
    if "débito" in t or "debito" in t: return "débito"
    if "crédito" in t or "credito" in t: return "crédito"
    return "Outros"

def detect_installments(text: str) -> str:
    """
    Condição de pagamento:
      - "à vista" se a frase contiver "à vista", "a vista" ou "avista"
      - "Nx" quando houver padrão de parcelamento (com ou sem espaço antes do 'x')
        Ex.: "em 10x", "10x", "parcelado em 12x", "21 x", "em 3 x"
      - Sempre normaliza para "Nx" (sem espaço)
    """
    t = text.lower()
    if re.search(r"\b(a\s+vista|à\s+vista|avista)\b", t):
        return "à vista"
    m = re.search(r"(?:parcelad[oa]\s*(?:em)?\s*|em\s*)?(\d{1,2})\s*x\b", t)
    if m:
        n = int(m.group(1))
        return f"{n}x"
    return "à vista"

def _category_before_comma(text: str) -> Optional[str]:
    if not text:
        return None
    parts = text.split(",", 1)
    if not parts:
        return None
    cat = parts[0].strip()
    if not cat:
        return None
    cat = re.sub(r"\s+", " ", cat)
    if cat.lower() in {"iptu", "ipva"}:
        return cat.upper()
    return _titlecase(cat)

# ===========================
# NLP (modo texto livre)
# ===========================
GROUP_EMOJI = {
    "GASTOS_FIXOS":      "🏠Gastos Fixos",
    "ASSINATURA":        "📺Assinatura",
    "GASTOS_VARIAVEIS":  "💸Gastos Variáveis",
    "DESPESAS_TEMP":     "🧾Despesas Temporárias",
    "PAG_FATURA":        "💳Pagamento de Fatura",
    "GANHOS":            "💵Ganhos",
    "INVESTIMENTO":      "💰Investimento",
    "RESERVA":           "📝Reserva",
    "SAQUE_RESGATE":     "💲Saque/Resgate",
}

def detect_group_and_category_free(text: str) -> Tuple[str, str]:
    t = text.lower()
    # Saque / Resgate
    if any(w in t for w in ["saquei", "resgatei", "resgate "]):
        cat = _category_before_comma(text) or "Saque/Resgate"
        return GROUP_EMOJI["SAQUE_RESGATE"], cat
    # Reserva
    if "reservei" in t or "reserva" in t:
        cat = _category_before_comma(text) or "Reserva"
        return GROUP_EMOJI["RESERVA"], cat
    # Investimento
    if "investi" in t or "investimento" in t:
        cat = _category_before_comma(text) or "Investimento"
        return GROUP_EMOJI["INVESTIMENTO"], cat
    # Pagamento de Fatura (só com termos específicos)
    if "pagamento de fatura" in t or "paguei a fatura" in t:
        cat = _category_before_comma(text) or "Cartão"
        return GROUP_EMOJI["PAG_FATURA"], cat
    # Ganhos
    if "vendas" in t:
        return GROUP_EMOJI["GANHOS"], "Vendas"
    if "salário" in t or "salario" in t:
        return GROUP_EMOJI["GANHOS"], "Salário"
    if "recebi" in t or "ganhei" in t:
        return GROUP_EMOJI["GANHOS"], "Ganhos"
    # Assinaturas
    assin = ["netflix", "amazon", "prime video", "disney", "disney+", "spotify", "globoplay", "youtube premium"]
    for a in assin:
        if a in t:
            return GROUP_EMOJI["ASSINATURA"], _titlecase(a)
    # Fixos
    if "aluguel" in t: return GROUP_EMOJI["GASTOS_FIXOS"], "Aluguel"
    if "água" in t or "agua" in t: return GROUP_EMOJI["GASTOS_FIXOS"], "Agua"
    if "energia" in t or "luz" in t: return GROUP_EMOJI["GASTOS_FIXOS"], "Energia"
    # Variáveis
    if "ifood" in t or "mercado" in t:
        return GROUP_EMOJI["GASTOS_VARIAVEIS"], "Mercado"
    # fallback
    return GROUP_EMOJI["GASTOS_VARIAVEIS"], _category_before_comma(text) or "Outros"

def parse_natural(text: str) -> Tuple[Optional[List], Optional[str]]:
    valor = parse_money(text)
    if valor is None:
        return None, "Não achei o valor."
    data_br = parse_date(text) or _format_date_br(_local_today())
    forma = detect_payment(text)
    cond = detect_installments(text)
    grupo, categoria = detect_group_and_category_free(text)
    # tipo (entrada/saída)
    if grupo in (GROUP_EMOJI["GANHOS"], GROUP_EMOJI["SAQUE_RESGATE"]):
        tipo = "▲ Entrada"
    else:
        tipo = "▼ Saída"
    desc = ""
    return [data_br, tipo, grupo, categoria, desc, valor, forma, cond], None
