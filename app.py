import os
import re
import json
import sqlite3
import secrets
import string
import logging
import unicodedata
from datetime import datetime, timedelta, timezone, date
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

# Google Auth: OAuth (sua conta) ou Service Account
GOOGLE_USE_OAUTH = os.getenv("GOOGLE_USE_OAUTH", "0") == "1"

GOOGLE_OAUTH_CLIENT_ID     = os.getenv("GOOGLE_OAUTH_CLIENT_ID")
GOOGLE_OAUTH_CLIENT_SECRET = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET")
GOOGLE_OAUTH_REDIRECT_URI  = os.getenv("GOOGLE_OAUTH_REDIRECT_URI")
GOOGLE_OAUTH_SCOPES = (os.getenv("GOOGLE_OAUTH_SCOPES") or
                       "https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/spreadsheets").split()
GOOGLE_TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "/tmp/google_oauth_token.json")
OAUTH_STATE_SECRET = os.getenv("OAUTH_STATE_SECRET", "change-me")

GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")  # caso use Service Account

# IDs do modelo e pasta (Drive)
GS_TEMPLATE_ID    = os.getenv("GS_TEMPLATE_ID")
GS_DEST_FOLDER_ID = os.getenv("GS_DEST_FOLDER_ID")

# Planilha de lançamentos (aba/intervalo)
WORKSHEET_NAME   = os.getenv("WORKSHEET_NAME", "🧾")
SHEET_FIRST_COL  = os.getenv("SHEET_FIRST_COL", "B")
SHEET_LAST_COL   = os.getenv("SHEET_LAST_COL", "I")
SHEET_START_ROW  = int(os.getenv("SHEET_START_ROW", "8"))

# Compartilhamento
SHARE_LINK_ROLE  = os.getenv("SHARE_LINK_ROLE", "writer")  # writer|commenter|reader

SCOPES_SA = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# ======== Licenças em Google Sheets =========
LICENSE_SHEET_ID  = os.getenv("LICENSE_SHEET_ID")
LICENSE_SHEET_TAB = os.getenv("LICENSE_SHEET_TAB", "Licencas")

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
        step TEXT,            -- 'await_license' | 'await_email' | 'await_text_grouped'
        temp_license TEXT,
        created_at TEXT
    )""")
    # Campos novos (idempotentes)
    try:
        cur.execute("ALTER TABLE clients ADD COLUMN email TEXT")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE pending ADD COLUMN extra TEXT")
    except Exception:
        pass
    con.commit()
    con.close()

def record_usage(chat_id, event):
    con = _db()
    con.execute("INSERT INTO usage(chat_id, event, ts) VALUES(?,?,?)",
                (str(chat_id), event, _now_iso()))
    con.commit(); con.close()

def _gen_key(prefix="GF"):
    alphabet = string.ascii_uppercase + string.digits
    part = lambda n: "".join(secrets.choice(alphabet) for _ in range(n))
    return f"{prefix}-{part(4)}-{part(4)}"

# ======== create_license usa Sheets se disponível =========
def create_license(days: Optional[int] = 30, max_files: int = 1, notes: Optional[str] = None, custom_key: Optional[str] = None):
    key = custom_key or _gen_key()

    if LICENSE_SHEET_ID:
        if _sheet_find_row_idx_by_license(key):
            while _sheet_find_row_idx_by_license(key):
                key = _gen_key()

        sheet_append_license(key, None if days == 0 else days, email=None)

        exp = None
        if days and days > 0:
            exp = (datetime.now(timezone.utc) + timedelta(days=days)).date().strftime("%Y-%m-%d")
        return key, exp

    # fallback SQLite
    expires_at = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat(timespec="seconds") if days else None
    con = _db()
    con.execute("INSERT INTO licenses(license_key,status,max_files,expires_at,notes) VALUES(?,?,?,?,?)",
                (key, "active", max_files, expires_at, notes))
    con.commit(); con.close()
    return key, expires_at

# ======== get_license lê do Sheets se disponível =========
def get_license(license_key: str):
    if LICENSE_SHEET_ID:
        return sheet_get_license(license_key)

    # fallback SQLite
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
    con.commit(); con.close()
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
    con.commit(); con.close()

def set_client_file(chat_id: str, item_id: str):
    con = _db()
    con.execute("""UPDATE clients SET file_scope=?, item_id=?, last_seen_at=? WHERE chat_id=?""",
                ("google", item_id, _now_iso(), str(chat_id)))
    con.commit(); con.close()

def set_pending(chat_id: str, step: Optional[str], temp_license: Optional[str]):
    con = _db()
    if step:
        con.execute("""
            INSERT INTO pending(chat_id, step, temp_license, created_at)
            VALUES(?,?,?,?)
            ON CONFLICT(chat_id) DO UPDATE SET step=excluded.step, temp_license=excluded.temp_license, created_at=excluded.created_at
        """, (str(chat_id), step, temp_license, _now_iso()))
    else:
        con.execute("DELETE FROM pending WHERE chat_id=?", (str(chat_id),))
    con.commit(); con.close()

def get_pending(chat_id: str):
    con = _db()
    cur = con.execute("SELECT step, temp_license FROM pending WHERE chat_id=?", (str(chat_id),))
    row = cur.fetchone()
    con.close()
    if not row:
        return None, None
    return row[0], row[1]

def set_pending_extra(chat_id: str, data: dict | None):
    con = _db()
    s = json.dumps(data) if data is not None else None
    con.execute("UPDATE pending SET extra=? WHERE chat_id=?", (s, str(chat_id)))
    con.commit(); con.close()

def get_pending_extra(chat_id: str) -> dict | None:
    con = _db()
    cur = con.execute("SELECT extra FROM pending WHERE chat_id=?", (str(chat_id),))
    row = cur.fetchone()
    con.close()
    if row and row[0]:
        try:
            return json.loads(row[0])
        except Exception:
            return None
    return None

def require_active_license(chat_id: str):
    cli = get_client(chat_id)
    if not cli:
        return False, "Para usar o bot você precisa **ativar sua licença**. Envie /start e siga as instruções."
    lic = get_license(cli["license_key"]) if cli["license_key"] else None
    ok, err = is_license_valid(lic)
    if not ok:
        return False, f"Licença inválida: {err}\nFale com o suporte para renovar/ativar."
    return True, None

# ===========================
# Telegram helpers
# ===========================
async def tg_send(chat_id, text):
    async with httpx.AsyncClient(timeout=12) as client:
        try:
            resp = await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error(f"Falha ao enviar msg ao Telegram (HTTP): {e}")
        except httpx.RequestError as e:
            logger.error(f"Falha ao enviar msg ao Telegram (Request): {e}")

def _tg_inline_keyboard(rows: list[list[tuple[str,str]]]):
    inline_keyboard = []
    for r in rows:
        inline_keyboard.append([{"text": lbl, "callback_data": data} for (lbl, data) in r])
    return {"inline_keyboard": inline_keyboard}

async def tg_send_with_kb(chat_id, text, kb):
    async with httpx.AsyncClient(timeout=12) as client:
        try:
            resp = await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown", "reply_markup": kb},
            )
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Falha ao enviar msg c/ teclado: {e}")

# pares (label mostrado, key interna)
GROUP_CHOICES = [
    ("💸Gastos Variáveis",   "GASTOS_VARIAVEIS"),
    ("🏠Gastos Fixos",       "GASTOS_FIXOS"),
    ("📺Assinatura",         "ASSINATURA"),
    ("🧾Despesas Temporárias","DESPESAS_TEMP"),
    ("💳Pagamento de Fatura","PAG_FATURA"),
    ("💵Ganhos",             "GANHOS"),
    ("💰Investimento",       "INVESTIMENTO"),
    ("📝Reserva",            "RESERVA"),
    ("💲Saque/Resgate",      "SAQUE_RESGATE"),
]

def _group_keyboard():
    rows = []
    row = []
    for i, (label, key) in enumerate(GROUP_CHOICES, 1):
        row.append((label, f"grp:{key}"))
        if i % 3 == 0:
            rows.append(row); row = []
    if row: rows.append(row)
    return _tg_inline_keyboard(rows)

def _group_label_by_key(k: str) -> str:
    for lbl, key in GROUP_CHOICES:
        if key == k:
            return lbl
    return "💸Gastos Variáveis"

# ===========================
# Google Auth helpers
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
    with open(GOOGLE_TOKEN_PATH, "w") as f:
        json.dump(data, f)

def _load_credentials() -> Optional[Credentials]:
    if not os.path.exists(GOOGLE_TOKEN_PATH):
        return None
    with open(GOOGLE_TOKEN_PATH, "r") as f:
        data = json.load(f)
    return Credentials.from_authorized_user_info(data, GOOGLE_OAUTH_SCOPES)

def _oauth_services():
    from google.auth.transport.requests import Request
    creds = _load_credentials()
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            _save_credentials(creds)
        else:
            logger.error("Autorização OAuth ausente ou inválida. Visite /oauth/start")
            raise RuntimeError("Autorize primeiro em /oauth/start")
    drive = build("drive", "v3", credentials=creds)
    sheets = build("sheets", "v4", credentials=creds)
    return drive, sheets

def _load_sa_json_tolerant(raw: str) -> dict:
    if not raw:
        raise RuntimeError("GOOGLE_SA_JSON não configurado.")
    s = raw.strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1]
    try:
        return json.loads(s)
    except Exception:
        try:
            s2 = bytes(s, "utf-8").decode("unicode_escape")
            return json.loads(s2)
        except Exception as e2:
            logger.error(f"Falha ao ler GOOGLE_SA_JSON: {e2}")
            raise RuntimeError(f"Falha ao ler GOOGLE_SA_JSON: {e2}")

def _sa_services():
    info = _load_sa_json_tolerant(GOOGLE_SA_JSON)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES_SA)
    drive = build("drive", "v3", credentials=creds)
    sheets = build("sheets", "v4", credentials=creds)
    return drive, sheets

def google_services():
    if GOOGLE_USE_OAUTH:
        return _oauth_services()
    return _sa_services()

# ===========================
# Google Drive/Sheets helpers
# ===========================
def drive_find_in_folder(service, folder_id: str, name: str) -> Optional[str]:
    safe_name = name.replace("'", "\\'")
    q = f"name = '{safe_name}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(
        q=q, spaces="drive", fields="files(id,name)", pageSize=1
    ).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None

def drive_copy_template(new_name: str) -> str:
    if not GS_TEMPLATE_ID or not GS_DEST_FOLDER_ID:
        raise RuntimeError("GS_TEMPLATE_ID e GS_DEST_FOLDER_ID devem estar configurados.")
    drive, _ = google_services()
    body = {
        "name": new_name,
        "parents": [GS_DEST_FOLDER_ID],
        "mimeType": "application/vnd.google-apps.spreadsheet",
    }
    file = drive.files().copy(fileId=GS_TEMPLATE_ID, body=body, fields="id").execute()
    return file["id"]

def drive_share_with_email(file_id: str, email: str, role: str = "writer") -> str:
    drive, _ = google_services()
    try:
        drive.permissions().create(
            fileId=file_id,
            body={"type": "user", "role": role, "emailAddress": email},
            fields="id"
        ).execute()
    except HttpError as e:
        if 'already has permission' not in str(e) and 'Domain policy' not in str(e):
            logger.error(f"Erro ao compartilhar {file_id} com {email}: {e}")
            raise
    meta = drive.files().get(fileId=file_id, fields="webViewLink").execute()
    return meta.get("webViewLink")

def drive_copy_and_link(email: str) -> Tuple[str, str]:
    new_name = f"Lancamentos - {email}"
    file_id = drive_copy_template(new_name)
    link = drive_share_with_email(file_id, email, SHARE_LINK_ROLE)
    return file_id, link

def sheets_append_row(spreadsheet_id: str, sheet_name: str, values: List):
    """
    Escreve SEM inserir linhas novas (preserva formatação/validação a partir da linha 8).
    Intervalo: ex. "🧾!B8:I"
    """
    _, sheets = google_services()
    rng = f"{sheet_name}!{SHEET_FIRST_COL}{SHEET_START_ROW}:{SHEET_LAST_COL}"
    body = {"values": [values]}
    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=rng,
        valueInputOption="USER_ENTERED",
        insertDataOption="OVERWRITE",
        body=body
    ).execute()

# ===========================
# ======= Licenças em Google Sheets
# ===========================
def _sheet_get_headers_and_rows():
    if not LICENSE_SHEET_ID:
        raise RuntimeError("LICENSE_SHEET_ID não configurado.")

    _, sheets = google_services()
    rng = f"{LICENSE_SHEET_TAB}!A:Z"
    resp = sheets.spreadsheets().values().get(
        spreadsheetId=LICENSE_SHEET_ID, range=rng, majorDimension="ROWS"
    ).execute()
    values = resp.get("values", [])
    if not values:
        raise RuntimeError("A aba de licenças está vazia (sem cabeçalho).")
    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    return headers, rows

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s.strip().lower()

def _sheet_header_index_map(headers):
    """
    Aceita: Licença | Validade | Data de inicio | Data final | email | status
    (case/acentos/ç ignorados)
    """
    idx = {_norm(h): i for i, h in enumerate(headers)}
    required = ["licenca", "validade", "data de inicio", "data final", "email", "status"]
    missing = [r for r in required if r not in idx]
    if missing:
        raise RuntimeError(f"Cabeçalho de licenças incompleto. Faltando: {', '.join(missing)}")
    return idx

def _sheet_find_row_idx_by_license(license_key: str) -> Optional[int]:
    headers, rows = _sheet_get_headers_and_rows()
    idx = _sheet_header_index_map(headers)
    col = idx["licenca"]
    for i, r in enumerate(rows, start=2):  # linha real (1=cabeçalho)
        if col < len(r) and (r[col] or "").strip().upper() == license_key.strip().upper():
            return i
    return None

def _col_letter(col_zero_based: int) -> str:
    col = col_zero_based + 1
    letters = ""
    while col:
        col, rem = divmod(col - 1, 26)
        letters = chr(65 + rem) + letters
    return letters

def sheet_update_license_email(license_key: str, email: str):
    if not LICENSE_SHEET_ID:
        return
    row = _sheet_find_row_idx_by_license(license_key)
    if not row:
        raise RuntimeError(f"Licença '{license_key}' não encontrada na planilha de licenças.")

    headers, _ = _sheet_get_headers_and_rows()
    idx = _sheet_header_index_map(headers)
    col_email = idx["email"]
    col_letter = _col_letter(col_email)
    rng = f"{LICENSE_SHEET_TAB}!{col_letter}{row}"

    _, sheets = google_services()
    sheets.spreadsheets().values().update(
        spreadsheetId=LICENSE_SHEET_ID,
        range=rng,
        valueInputOption="USER_ENTERED",
        body={"values": [[email]]}
    ).execute()

def sheet_get_license(license_key: str) -> Optional[dict]:
    headers, rows = _sheet_get_headers_and_rows()
    idx = _sheet_header_index_map(headers)

    for r in rows:
        key = (r[idx["licenca"]] if idx["licenca"] < len(r) else "").strip().upper()
        if key == license_key.strip().upper():
            status = (r[idx["status"]] if idx["status"] < len(r) else "").strip().lower() or "active"
            end    = (r[idx["data final"]] if idx["data final"] < len(r) else "").strip()
            expires_at = None
            if end:
                expires_at = f"{end}T23:59:59+00:00"
            return {
                "license_key": license_key,
                "status": status,
                "max_files": 1,
                "expires_at": expires_at,
                "notes": None,
            }
    return None

def sheet_append_license(license_key: str, days: Optional[int], email: Optional[str] = None):
    start_date = datetime.now(timezone.utc).date()
    start_iso = start_date.strftime("%Y-%m-%d")
    end_iso   = (start_date + timedelta(days=days)).strftime("%Y-%m-%d") if days else ""

    _, sheets = google_services()
    values = [[
        license_key,
        "" if (days is None or days == 0) else str(days),
        start_iso,
        end_iso,
        email or "",
        "active",
    ]]
    rng = f"{LICENSE_SHEET_TAB}!A:F"
    sheets.spreadsheets().values().append(
        spreadsheetId=LICENSE_SHEET_ID,
        range=rng,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": values},
    ).execute()

# ===========================
# NLP / Parsing
# ===========================

# --- Etiquetas de grupos (sem espaço após o emoji) ---
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

TRAILING_STOP = {
    "hoje", "ontem", "amanha", "amanhã", "agora", "hj",
    "via", "no", "na", "em", "de", "do", "da", "e",
    "pix", "débito", "debito", "crédito", "credito"
}

MERCHANT_MAP = {
    "ifood": ("GASTOS_VARIAVEIS", "ifood"),
    "uber": ("GASTOS_VARIAVEIS", "Uber"),
    "99": ("GASTOS_VARIAVEIS", "99"),
    "rappi": ("GASTOS_VARIAVEIS", "Rappi"),
    "magazine luiza": ("GASTOS_VARIAVEIS", "Magazine Luiza"),
    "magalu": ("GASTOS_VARIAVEIS", "Magazine Luiza"),
    "amazon": ("GASTOS_VARIAVEIS", "Amazon"),
    "mercado livre": ("GASTOS_VARIAVEIS", "Mercado Livre"),
    "mercado": ("GASTOS_VARIAVEIS", "mercado"),
}

KEYWORDS_FIXOS = {
    "aluguel": ("GASTOS_FIXOS", "aluguel"),
    "água": ("GASTOS_FIXOS", "Agua"),
    "agua": ("GASTOS_FIXOS", "Agua"),
    "energia": ("GASTOS_FIXOS", "Energia"),
    "luz": ("GASTOS_FIXOS", "Energia"),
    "internet": ("GASTOS_FIXOS", "Internet"),
    "condomínio": ("GASTOS_FIXOS", "Condomínio"),
    "condominio": ("GASTOS_FIXOS", "Condomínio"),
}

ASSINATURAS = {
    "netflix": ("ASSINATURA", "Netflix"),
    "amazon": ("ASSINATURA", "Amazon"),
    "amazon prime": ("ASSINATURA", "Amazon"),
    "prime video": ("ASSINATURA", "Amazon"),
    "disney": ("ASSINATURA", "Disney+"),
    "disney+": ("ASSINATURA", "Disney+"),
    "globoplay": ("ASSINATURA", "Globoplay"),
    "spotify": ("ASSINATURA", "Spotify"),
    "hbo": ("ASSINATURA", "HBO"),
    "max": ("ASSINATURA", "Max"),
    "apple tv": ("ASSINATURA", "Apple TV+"),
    "youtube premium": ("ASSINATURA", "YouTube Premium"),
}

def _titlecase(s: str) -> str:
    return " ".join(w.capitalize() for w in (s or "").split())

def _format_date_br(d: date) -> str:
    return d.strftime("%d/%m/%Y")

def parse_date(text: str) -> Optional[str]:
    t = text.lower()
    today = datetime.now().date()
    if "hoje" in t: return _format_date_br(today)
    if "ontem" in t: return _format_date_br(today - timedelta(days=1))
    if "amanh" in t: return _format_date_br(today + timedelta(days=1))
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
    t = text.lower()
    t = t.replace("r$", " ").replace("reais", " ")

    # remove datas para não capturar "01" de "01/10"
    t = re.sub(r"\b\d{1,2}[\/\-.]\d{1,2}(?:[\/\-.]\d{2,4})?\b", " ", t)

    # captura todos os números plausíveis; escolhe o último (normalmente o valor monetário)
    pattern = re.compile(r"\b\d{1,3}(?:[.\s]\d{3})*(?:,\d{1,2})\b|\b\d+(?:[.,]\d{1,2})\b|\b\d+\b")
    matches = list(pattern.finditer(t))
    if not matches:
        return None

    raw = matches[-1].group(0)
    raw = raw.replace(" ", "")
    if "," in raw and "." in raw:
        raw = raw.replace(".", "").replace(",", ".")
    else:
        raw = raw.replace(",", ".")
    try:
        return float(raw)
    except:
        return None

def _clean_trailing_tokens(s: str) -> str:
    tokens = s.split()
    while tokens and tokens[-1].lower() in TRAILING_STOP:
        tokens.pop()
    return " ".join(tokens).strip()

def detect_installments(text: str) -> str:
    t = text.lower()
    m = re.search(r"(\d{1,2})\s*x", t)
    if m: return f"{m.group(1)}x"
    m = re.search(r"(\d{1,2})\s*de\s*(\d{1,2})", t)
    if m: return f"{m.group(2)}x"
    if "parcelad" in t: return "parcelado"
    if "à vista" in t or "a vista" in t or "avista" in t: return "à vista"
    return "à vista"

def detect_payment(text: str) -> str:
    t = text.lower()
    m = re.search(r"cart[aã]o\s+([a-z0-9 ]+)", t)
    if m:
        brand = _clean_trailing_tokens(m.group(1))
        brand = re.sub(r"\s+", " ", brand).strip()
        if brand:
            return f"💳cartão {_titlecase(brand)}"
        return "💳cartão"
    if "pix" in t: return "Pix"
    if "dinheiro" in t or "cash" in t: return "Dinheiro"
    if "débito" in t or "debito" in t: return "débito"
    if "crédito" in t or "credito" in t: return "Crédito"
    return "Outros"

def _strip_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _extract_colon_category(text: str) -> Optional[str]:
    """
    Captura o trecho entre ':' e a próxima ',' (ou fim da string).
    Remove espaços imediatamente após ':'.
    """
    m = re.search(r":\s*([^,]+)", text)
    if m:
        return _strip_spaces(m.group(1))
    return None

def _extract_free_text_after_keywords(text: str, keywords: List[str]) -> Optional[str]:
    t = text.strip()
    for kw in keywords:
        i = t.lower().find(kw)
        if i >= 0:
            frag = t[i+len(kw):].strip(" .,:;-")
            frag = re.split(r"\b(via|no|na|em|de|do|da|valor|no valor)\b", frag, maxsplit=1)[0].strip()
            if frag:
                return frag
    return None

def _has_fatura_keywords(t: str) -> bool:
    t = t.lower()
    return ("pagamento de fatura" in t) or ("paguei a fatura" in t)

def _detect_pagamento_fatura(text: str):
    """
    Só é Pagamento de Fatura se houver as palavras-chave ("pagamento de fatura" ou "paguei a fatura").
    Se houver, categoria usa ":" se existir; senão tenta extrair com regex de cartão.
    """
    if not _has_fatura_keywords(text):
        return None, None

    cat_from_colon = _extract_colon_category(text)
    if cat_from_colon:
        return GROUP_EMOJI["PAG_FATURA"], cat_from_colon

    t = text.lower()
    m = re.search(r"fatura\s+do?\s*cart[aã]o\s+([a-z0-9 ]+)", t)
    if m:
        brand = _clean_trailing_tokens(m.group(1))
        brand = _titlecase(brand) if brand else "cartão"
        return GROUP_EMOJI["PAG_FATURA"], f"cartão {brand}"

    return GROUP_EMOJI["PAG_FATURA"], "cartão"

def _detect_saque_resgate(text: str):
    t = text.lower()
    if any(w in t for w in ["saquei", "saque ", "resgatei", "resgate "]):
        cat = _extract_colon_category(text)
        if not cat:
            np = _extract_free_text_after_keywords(text, ["saquei", "resgatei", "saque", "resgate"])
            cat = np if np else "Saque/Resgate"
        return GROUP_EMOJI["SAQUE_RESGATE"], cat
    return None, None

def detect_group_and_category(text: str) -> Tuple[str, str]:
    t = text.lower()

    # PRIORIDADE:
    # 1) Saque/Resgate
    grp, cat = _detect_saque_resgate(text)
    if grp: return grp, cat

    # 2) Reserva
    if any(w in t for w in ["reservei", "reserva "]) or re.search(r"\breservei\b|\breserva\b", t):
        cat2 = _extract_colon_category(text)
        if not cat2:
            cat2 = _extract_free_text_after_keywords(text, ["reservei", "reserva"]) or "Reserva"
        return GROUP_EMOJI["RESERVA"], cat2

    # 3) Investimento
    if ("investi" in t) or ("investimento" in t):
        if "renda fixa" in t:
            categoria = "renda fixa"
        elif "aç" in t or "aco" in t or "ações" in t or "acoes" in t:
            categoria = "Ações"
        else:
            m = re.search(r"\binv\w*\s+(?:em\s+)?([a-z0-9 ]+)", t)
            categoria = _titlecase(_clean_trailing_tokens(m.group(1))) if m else "Investimento"
        return GROUP_EMOJI["INVESTIMENTO"], categoria

    # 4) Pagamento de Fatura (apenas com palavras-chave)
    grp, cat = _detect_pagamento_fatura(text)
    if grp: return grp, cat

    # 5) Assinaturas
    for k, (gkey, cat_disp) in ASSINATURAS.items():
        if k in t:
            return GROUP_EMOJI[gkey], cat_disp

    # 6) Ganhos (prioriza "vendas")
    if "vendas" in t:
        return GROUP_EMOJI["GANHOS"], "Vendas"
    if "salário" in t or "salario" in t:
        return GROUP_EMOJI["GANHOS"], "salário"
    if re.search(r"\b(recebi|ganhei)\b", t):
        return GROUP_EMOJI["GANHOS"], "Ganhos"

    # 7) Fixos
    for k, (gkey, cat_disp) in KEYWORDS_FIXOS.items():
        if k in t:
            return GROUP_EMOJI[gkey], cat_disp

    # 8) Variáveis (mapeadas)
    for k, (gkey, cat_disp) in MERCHANT_MAP.items():
        if k in t:
            return GROUP_EMOJI[gkey], cat_disp

    # 9) fallback geral
    if any(w in t for w in ["mercado", "restaurante", "lanche", "pizza", "hamburg", "sushi", "ifood", "rappi"]):
        return GROUP_EMOJI["GASTOS_VARIAVEIS"], "mercado" if "mercado" in t else "Outros"

    return GROUP_EMOJI["GASTOS_VARIAVEIS"], "Outros"

def parse_natural(text: str) -> Tuple[Optional[List], Optional[str]]:
    valor = parse_money(text)
    if valor is None:
        return None, "Não achei o valor. Ex.: 45,90"

    data_br = parse_date(text) or _format_date_br(datetime.now().date())
    forma = detect_payment(text)
    cond = detect_installments(text)

    group, category = detect_group_and_category(text)
    t_low = text.lower()

    # Tipo:
    # - Saque/Resgate, Ganhos, Pagamento de fatura => Entrada
    # - Investimento => sempre Saída
    if group == GROUP_EMOJI["INVESTIMENTO"]:
        tipo = "▼ Saída"
    elif group in (GROUP_EMOJI["SAQUE_RESGATE"], GROUP_EMOJI["GANHOS"], GROUP_EMOJI["PAG_FATURA"]) \
         or re.search(r"\b(ganhei|recebi|sal[aá]rio|renda)\b", t_low):
        tipo = "▲ Entrada"
    else:
        tipo = "▼ Saída"

    # Em Pagamento de Fatura, forma NUNCA é "💳cartão ..."
    if group == GROUP_EMOJI["PAG_FATURA"] and str(forma).startswith("💳cartão"):
        if "pix" in t_low:
            forma = "Pix"
        elif ("débito" in t_low) or ("debito" in t_low):
            forma = "débito"
        else:
            forma = "Outros"

    # Descrição: sempre vazia
    desc = ""

    return [data_br, tipo, group, category, desc, float(valor), forma, cond], None

# ===========================
# Provisionamento (Google)
# ===========================
def _ensure_unique_or_reuse(email: str) -> Optional[str]:
    if not GS_DEST_FOLDER_ID:
        return None
    drive, _ = google_services()
    name = f"Lancamentos - {email}"
    return drive_find_in_folder(drive, GS_DEST_FOLDER_ID, name)

async def setup_client_file(chat_id: str, email: str) -> Tuple[bool, Optional[str], Optional[str]]:
    cli = get_client(chat_id)
    if cli and cli.get("item_id"):
        try:
            link = drive_share_with_email(cli["item_id"], email, SHARE_LINK_ROLE)
        except Exception:
            link = None
        return True, None, link

    try:
        exist_id = _ensure_unique_or_reuse(email)
        if exist_id:
            set_client_file(str(chat_id), exist_id)
            try:
                link = drive_share_with_email(exist_id, email, SHARE_LINK_ROLE)
            except Exception:
                link = None
            return True, None, link

        new_id, web_link = drive_copy_and_link(email)
        set_client_file(str(chat_id), new_id)
        return True, None, web_link

    except HttpError as e:
        logger.error(f"HttpError na API Google: {e}")
        return False, f"Falha Google API: {e}", None
    except Exception as e:
        logger.error(f"Exceção ao criar planilha: {e}")
        return False, f"Falha ao criar planilha: {e}", None

def add_row_to_client(values: List, chat_id: str):
    if len(values) != 8:
        raise RuntimeError(f"Esperava 8 colunas, recebi {len(values)}.")
    cli = get_client(chat_id)
    if not cli or not cli.get("item_id"):
        raise RuntimeError("Planilha do cliente não configurada.")
    sheets_append_row(cli["item_id"], WORKSHEET_NAME, values)

# ===========================
# Rotas
# ===========================
@app.on_event("startup")
def _startup():
    licenses_db_init()
    print(f"✅ DB pronto em {SQLITE_PATH}")
    print(f"Auth mode: {'OAuth' if GOOGLE_USE_OAUTH else 'Service Account'}")

@app.get("/")
def root():
    return {"status": "ok", "auth_mode": "oauth" if GOOGLE_USE_OAUTH else "sa"}

@app.get("/ping")
def ping():
    return {"pong": True}

# ---- OAuth flow (se usar GOOGLE_USE_OAUTH=1) ----
@app.get("/oauth/start")
def oauth_start():
    if not GOOGLE_USE_OAUTH:
        return HTMLResponse("<h3>OAuth desabilitado. Defina GOOGLE_USE_OAUTH=1.</h3>", status_code=400)
    if not (GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET and GOOGLE_OAUTH_REDIRECT_URI):
        return HTMLResponse("<h3>Faltam variáveis do OAuth no ambiente.</h3>", status_code=500)
    flow = Flow.from_client_config(_client_config_dict(), scopes=GOOGLE_OAUTH_SCOPES, redirect_uri=GOOGLE_OAUTH_REDIRECT_URI)
    auth_url, state = flow.authorization_url(access_type="offline", include_granted_scopes="true", prompt="consent", state=OAUTH_STATE_SECRET)
    return RedirectResponse(auth_url)

@app.get("/oauth/callback")
def oauth_callback(code: str | None = None, state: str | None = None):
    if not GOOGLE_USE_OAUTH:
        return HTMLResponse("<h3>OAuth desabilitado. Defina GOOGLE_USE_OAUTH=1.</h3>", status_code=400)
    if state != OAUTH_STATE_SECRET:
        return HTMLResponse("<h3>State inválido.</h3>", status_code=400)
    if not code:
        return HTMLResponse("<h3>Faltou 'code'.</h3>", status_code=400)
    flow = Flow.from_client_config(_client_config_dict(), scopes=GOOGLE_OAUTH_SCOPES, redirect_uri=GOOGLE_OAUTH_REDIRECT_URI)
    flow.fetch_token(code=code)
    creds = flow.credentials
    if not creds.refresh_token:
        return HTMLResponse("<h3>Não veio refresh_token. Refazer /oauth/start.</h3>", status_code=400)
    _save_credentials(creds)
    return HTMLResponse("<h3>✅ OAuth ok! Pode voltar ao Telegram.</h3>")

# ---- Telegram webhook ----
@app.post("/telegram/webhook")
async def telegram_webhook(
    req: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(default=None)
):
    if TELEGRAM_WEBHOOK_SECRET:
        if (x_telegram_bot_api_secret_token or "") != TELEGRAM_WEBHOOK_SECRET:
            return {"ok": True}

    body = await req.json()

    # ===== CallbackQuery (botões inline) =====
    callback = body.get("callback_query")
    if callback:
        cb_id = callback.get("id")
        chat_id_cb = callback.get("message", {}).get("chat", {}).get("id")
        data_cb = (callback.get("data") or "").strip()

        # confirma ao Telegram (evita "loading...")
        try:
            async with httpx.AsyncClient(timeout=12) as client:
                await client.post(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
                    json={"callback_query_id": cb_id}
                )
        except Exception:
            pass

        if data_cb.startswith("grp:"):
            grp_key = data_cb.split(":")[1]
            set_pending(str(chat_id_cb), "await_text_grouped", None)
            set_pending_extra(str(chat_id_cb), {"group": grp_key})
            label = _group_label_by_key(grp_key)
            await tg_send(chat_id_cb, f"✔️ Grupo selecionado: *{label}*.\nAgora me envie o lançamento (ex.: `59,90 no débito hoje`).")
            return {"ok": True}

        return {"ok": True}

    # ===== Mensagem normal =====
    message = body.get("message") or {}
    chat_id = message.get("chat", {}).get("id")
    text = (message.get("text") or "").strip()
    if not chat_id or not text:
        return {"ok": True}
    chat_id_str = str(chat_id)

    # Admin
    if ADMIN_TELEGRAM_ID and chat_id_str == ADMIN_TELEGRAM_ID:
        low = text.lower()
        if low.startswith("/licenca nova"):
            parts = text.split()
            custom_key = None
            days = 30
            try:
                if len(parts) >= 4 and parts[2] and parts[3].isdigit():
                    custom_key = parts[2].strip()
                    days = int(parts[3])
                elif len(parts) >= 3 and parts[2].isdigit():
                    days = int(parts[2])
            except Exception:
                pass
            key, exp = create_license(days=None if days == 0 else days, custom_key=custom_key)
            msg = f"🔑 *Licença criada:*\n`{key}`\n*Validade:* {'vitalícia' if not exp else exp}"
            await tg_send(chat_id, msg)
            return {"ok": True}

        if low.startswith("/licenca info"):
            await tg_send(chat_id, f"Seu ADMIN ID ({chat_id_str}) está correto. O bot está ativo.")
            return {"ok": True}

        if low.startswith("/licenca"):
            await tg_send(chat_id, "Comando de licença não reconhecido ou incompleto.")
            return {"ok": True}

    # /cancel
    if text.lower() == "/cancel":
        set_pending(chat_id_str, None, None)
        set_pending_extra(chat_id_str, None)
        await tg_send(chat_id, "Operação cancelada. Envie /start para começar novamente.")
        return {"ok": True}

    # /novo -> teclado de grupos
    if text.lower() in ("/novo", "/lancar", "/lançar"):
        kb = _group_keyboard()
        await tg_send_with_kb(chat_id, "O que você quer lançar? Escolha o *grupo* abaixo:", kb)
        return {"ok": True}

    # /start amigável
    if text.lower() == "/start":
        record_usage(chat_id, "start")
        set_pending(chat_id_str, "await_license", None)
        set_pending_extra(chat_id_str, None)
        await tg_send(chat_id,
            "Olá! 👋\nPor favor, *informe sua licença* (ex.: `GF-ABCD-1234`).\n\n"
            "Você pode digitar /novo para lançar escolhendo o *grupo* pelos botões.\n"
            "Você pode digitar /cancel para cancelar."
        )
        return {"ok": True}

    # /start TOKEN [email] (fallback)
    if text.lower().startswith("/start "):
        record_usage(chat_id, "start_token")
        parts = text.split()
        token = parts[1].strip() if len(parts) >= 2 else None
        email = parts[2].strip() if len(parts) >= 3 else None

        if not token:
            await tg_send(chat_id, "Envie `/start SEU-CÓDIGO` (ex.: `/start GF-ABCD-1234`).")
            return {"ok": True}

        lic = get_license(token)
        ok, err = is_license_valid(lic)
        if not ok:
            await tg_send(chat_id, f"❌ Licença inválida: {err}")
            return {"ok": True}

        ok2, err2 = bind_license_to_chat(chat_id_str, token)
        if not ok2:
            await tg_send(chat_id, f"❌ {err2}")
            return {"ok": True}

        if not email:
            set_pending(chat_id_str, "await_email", token)
            await tg_send(chat_id, "Licença ok ✅\nAgora me diga seu *e-mail* (ex.: `cliente@gmail.com`).")
            return {"ok": True}

        set_client_email(chat_id_str, email)
        try:
            if LICENSE_SHEET_ID:
                sheet_update_license_email(token, email)
        except Exception as e:
            logger.error(f"Falha ao atualizar e-mail da licença no Sheets: {e}")

        await tg_send(chat_id, "✅ Obrigado! Configurando sua planilha de lançamentos...")

        okf, errf, link = await setup_client_file(chat_id_str, email)
        if not okf:
            logger.error(f"ERRO CRÍTICO NO SETUP DO ARQUIVO: {errf}")
            await tg_send(chat_id, f"❌ Falha na configuração: {errf}. Verifique os logs do servidor.")
            return {"ok": True}

        await tg_send(chat_id, f"🚀 Planilha configurada com sucesso!\n🔗 {link}")
        await tg_send(chat_id, "Agora pode me contar seus gastos digitando livremente ou use /novo para escolher o grupo.")
        return {"ok": True}

    # Conversa pendente
    step, temp_license = get_pending(chat_id_str)
    if step == "await_license":
        token = text.strip()
        lic = get_license(token)
        ok, err = is_license_valid(lic)
        if not ok:
            await tg_send(chat_id, f"❌ Licença inválida: {err}\nTente novamente ou digite /cancel.")
            return {"ok": True}
        ok2, err2 = bind_license_to_chat(chat_id_str, token)
        if not ok2:
            await tg_send(chat_id, f"❌ {err2}\nTente novamente ou digite /cancel.")
            return {"ok": True}
        set_pending(chat_id_str, "await_email", token)
        await tg_send(chat_id, "Licença ok ✅\nAgora me diga seu *e-mail* (ex.: `cliente@gmail.com`).")
        return {"ok": True}

    if step == "await_email":
        email = text.strip()
        if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
            await tg_send(chat_id, "❗ E-mail inválido. Tente novamente (ex.: `cliente@gmail.com`).")
            return {"ok": True}
        set_client_email(chat_id_str, email)
        try:
            if LICENSE_SHEET_ID and temp_license:
                sheet_update_license_email(temp_license, email)
        except Exception as e:
            logger.error(f"Falha ao atualizar e-mail da licença no Sheets: {e}")

        set_pending(chat_id_str, None, None)
        set_pending_extra(chat_id_str, None)
        await tg_send(chat_id, "✅ Obrigado! Configurando sua planilha de lançamentos...")

        okf, errf, link = await setup_client_file(chat_id_str, email)
        if not okf:
            logger.error(f"ERRO CRÍTICO NO SETUP DO ARQUIVO: {errf}")
            await tg_send(chat_id, f"❌ Falha na configuração: {errf}. Verifique os logs do servidor.")
            return {"ok": True}

        await tg_send(chat_id, f"🚀 Planilha configurada com sucesso!\n🔗 {link}")
        await tg_send(chat_id, "Agora pode me contar seus gastos digitando livremente ou use /novo para escolher o grupo.")
        return {"ok": True}

    if step == "await_text_grouped":
        extra = get_pending_extra(chat_id_str) or {}
        forced_group_key = extra.get("group")
        if not forced_group_key:
            kb = _group_keyboard()
            await tg_send_with_kb(chat_id, "Escolha um *grupo* primeiro:", kb)
            return {"ok": True}

        ok, msg = require_active_license(chat_id_str)
        if not ok:
            await tg_send(chat_id, f"❗ {msg}")
            return {"ok": True}

        row, err = parse_natural(text)
        if err:
            await tg_send(chat_id, f"❗ {err}")
            return {"ok": True}

        # row: [data_br, tipo, group_label, category, desc, valor, forma, cond]
        group_label = GROUP_EMOJI.get(forced_group_key, GROUP_EMOJI["GASTOS_VARIAVEIS"])
        row[2] = group_label

        # Ajusta TIPO conforme grupo
        if forced_group_key == "INVESTIMENTO":
            row[1] = "▼ Saída"
        elif forced_group_key in ("SAQUE_RESGATE", "GANHOS", "PAG_FATURA"):
            row[1] = "▲ Entrada"
        else:
            row[1] = "▼ Saída"

        # Em pagamento de fatura, forma jamais é "💳cartão ..."
        t_low = text.lower()
        if forced_group_key == "PAG_FATURA" and str(row[6]).startswith("💳cartão"):
            if "pix" in t_low:
                row[6] = "Pix"
            elif ("débito" in t_low) or ("debito" in t_low):
                row[6] = "débito"
            else:
                row[6] = "Outros"

        try:
            add_row_to_client(row, chat_id_str)
            set_pending(chat_id_str, None, None)
            set_pending_extra(chat_id_str, None)
            await tg_send(chat_id, "✅ Lançado!")
        except Exception as e:
            logger.error(f"Erro ao lançar forçado por grupo: {e}")
            await tg_send(chat_id, f"❌ Erro ao lançar na planilha: {e}")
        return {"ok": True}

    # exige licença (modo texto livre sem /novo)
    ok, msg = require_active_license(chat_id_str)
    if not ok:
        await tg_send(chat_id, f"❗ {msg}")
        return {"ok": True}

    # Lançamento modo livre
    row, err = parse_natural(text)
    if err:
        await tg_send(chat_id, f"❗ {err}")
        return {"ok": True}
    try:
        add_row_to_client(row, chat_id_str)
        await tg_send(chat_id, "✅ Lançado!")
    except Exception as e:
        logger.error(f"Erro ao lançar na planilha: {e}")
        await tg_send(chat_id, f"❌ Erro ao lançar na planilha: {e}")

    return {"ok": True}
