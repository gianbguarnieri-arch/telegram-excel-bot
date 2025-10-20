import os
import re
import json
import sqlite3
import secrets
import string
import logging
import unicodedata  # NEW
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
        step TEXT,            -- 'await_license' | 'await_email'
        temp_license TEXT,
        created_at TEXT
    )""")
    try:
        cur.execute("ALTER TABLE clients ADD COLUMN email TEXT")
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
    """
    Se LICENSE_SHEET_ID estiver definido → cria/append na planilha.
    Caso contrário, mantém o comportamento anterior (SQLite).
    """
    key = custom_key or _gen_key()

    if LICENSE_SHEET_ID:
        # evita colisão improvável
        if _sheet_find_row_idx_by_license(key):
            while _sheet_find_row_idx_by_license(key):
                key = _gen_key()

        sheet_append_license(key, None if days == 0 else days, email=None)

        # valor exibido ao admin: iso data final ou 'vitalícia'
        exp = None
        if days and days > 0:
            exp = (datetime.now(timezone.utc) + timedelta(days=days)).date().strftime("%Y-%m-%d")
        return key, exp

    # --- fallback SQLite (como antes) ---
    expires_at = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat(timespec="seconds") if days else None
    con = _db()
    con.execute("INSERT INTO licenses(license_key,status,max_files,expires_at,notes) VALUES(?,?,?,?,?)",
                (key, "active", max_files, expires_at, notes))
    con.commit(); con.close()
    return key, expires_at

# ======== get_license lê do Sheets se disponível =========
def get_license(license_key: str):
    """
    Se LICENSE_SHEET_ID estiver definido → lê da planilha.
    Caso contrário, mantém o comportamento anterior (SQLite).
    """
    if LICENSE_SHEET_ID:
        return sheet_get_license(license_key)

    # --- fallback SQLite ---
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
# Telegram helper
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
        insertDataOption="OVERWRITE",  # não insere linha; escreve na próxima vazia da faixa
        body=body
    ).execute()

# ===========================
# ======= Licenças em Google Sheets
# ===========================
def _sheet_get_headers_and_rows():
    """
    Lê cabeçalho (linha 1) e todas as linhas seguintes da aba de licenças.
    Requer LICENSE_SHEET_ID configurado.
    """
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

def _sheet_header_index_map(headers):
    """
    Aceita estes nomes (case/acento ignorados):
      Licença | Validade | Data de inicio | Data final | email | status
    """
    def norm(s: str) -> str:
        s = unicodedata.normalize("NFD", s or "")
        s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")  # remove acentos/ç
        return s.strip().lower()
    idx = {norm(h): i for i, h in enumerate(headers)}
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
    # A..Z suficiente aqui (A-F), mas deixo generalizado
    col = col_zero_based + 1
    letters = ""
    while col:
        col, rem = divmod(col - 1, 26)
        letters = chr(65 + rem) + letters
    return letters

def sheet_update_license_email(license_key: str, email: str):
    """
    Atualiza a célula da coluna 'email' na linha da licença informada.
    """
    if not LICENSE_SHEET_ID:
        return  # nada a fazer se não usa planilha de licenças

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
    """
    Retorna no formato esperado por is_license_valid():
      { license_key, status, max_files, expires_at, notes }
    Onde expires_at é ISO datetime com TZ (ex.: '2025-11-19T23:59:59+00:00') ou None.
    """
    headers, rows = _sheet_get_headers_and_rows()
    idx = _sheet_header_index_map(headers)

    for r in rows:
        key = (r[idx["licenca"]] if idx["licenca"] < len(r) else "").strip().upper()
        if key == license_key.strip().upper():
            status = (r[idx["status"]] if idx["status"] < len(r) else "").strip().lower() or "active"
            end    = (r[idx["data final"]] if idx["data final"] < len(r) else "").strip()
            expires_at = None
            if end:
                # transforma 'YYYY-MM-DD' em 'YYYY-MM-DDT23:59:59+00:00'
                expires_at = f"{end}T23:59:59+00:00"
            return {
                "license_key": license_key,
                "status": status,
                "max_files": 1,   # planilha não controla isso por enquanto
                "expires_at": expires_at,
                "notes": None,
            }
    return None

def sheet_append_license(license_key: str, days: Optional[int], email: Optional[str] = None):
    """
    Insere nova linha: Licença | Validade(dias) | Data de inicio | Data final | email | status
    """
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
def parse_money(text: str) -> Optional[float]:
    m = re.search(r"(\d{1,3}(?:\.\d{3})*(?:,\d{2})|\d+(?:\.\d{2})?)", text)
    if not m: return None
    val = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(val)
    except:
        return None

def parse_date(text: str) -> Optional[str]:
    t = text.lower()
    today = datetime.now().date()
    if "hoje" in t: return today.strftime("%Y-%m-%d")
    if "ontem" in t: return (today - timedelta(days=1)).strftime("%Y-%m-%d")
    m = re.search(r"\b(\d{1,2})/(\d{1,2})(?:/(\d{4}))?\b", text)
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3) or str(today.year)
        try:
            dt = datetime.strptime(f"{d}/{mo}/{y}", "%d/%m/%Y").date()
            return dt.strftime("%Y-%m-%d")
        except: return None
    return None

# ===== Grupos com ícones =====
GROUP_EMOJI = {
    "GASTOS_FIXOS":      "🏠 Gastos Fixos",
    "ASSINATURA":        "📺 Assinatura",
    "GASTOS_VARIAVEIS":  "💸 Gastos Variáveis",
    "DESPESAS_TEMP":     "🧾 Despesas Temporárias",
    "PAG_FATURA":        "💳 Pagamento de Fatura",
    "GANHOS":            "💵 Ganhos",
    "INVESTIMENTO":      "💰 Investimento",
    "RESERVA":           "📝 Reserva",
    "SAQUE_RESGATE":     "💲 Saque/Resgate",
}

TRAILING_STOP = {
    "hoje", "ontem", "amanha", "amanhã", "agora", "hj",
    "via", "no", "na", "em", "de", "do", "da", "e",
    "pix", "débito", "debito", "crédito", "credito"
}

ASSINATURAS = {
    "netflix": "Netflix",
    "spotify": "Spotify",
    "disney": "Disney+",
    "amazon prime": "Amazon Prime",
    "prime video": "Amazon Prime",
    "globoplay": "Globoplay",
    "hbo": "HBO",
    "max": "Max",
    "apple tv": "Apple TV+",
    "youtube premium": "YouTube Premium",
}

KEYWORDS_FIXOS = {"aluguel", "condomínio", "condominio", "luz", "energia", "água", "agua", "internet"}
KEYWORDS_TEMP = {"iptu", "ipva", "multa", "empréstimo", "emprestimo"}
KEYWORDS_VARIAVEIS = {"mercado", "farmácia", "farmacia", "combustível", "combustivel", "restaurante", "lanche",
                      "pizza", "hamburguer", "sushi", "ifood", "cinema", "passeio", "hotel", "viagem"}
KEYWORDS_GANHOS = {"salário", "salario", "recebi", "ganhei", "renda", "pró labore", "pro labore"}

def _titlecase(s: str) -> str:
    return " ".join(w.capitalize() for w in s.split())

def _clean_trailing_tokens(s: str) -> str:
    tokens = s.split()
    while tokens and tokens[-1].lower() in TRAILING_STOP:
        tokens.pop()
    return " ".join(tokens).strip()

def detect_payment(text: str) -> str:
    t = text.lower()
    m = re.search(r"cart[aã]o\s+([a-z0-9 ]+)", t)
    if m:
        brand = _clean_trailing_tokens(m.group(1))
        brand = re.sub(r"\s+", " ", brand).strip()
        if brand:
            return f"💳 cartão {_titlecase(brand)}"
        return "💳 cartão"
    if "pix" in t: return "Pix"
    if "dinheiro" in t or "cash" in t: return "Dinheiro"
    if "débito" in t ou "debito" in t: return "Débito"
    if "crédito" in t ou "credito" in t: return "Crédito"
    return "Outros"

def _detect_pagamento_fatura(text: str):
    t = text.lower()
    m = re.search(r"pague[iu]\s+a?\s*fatura\s+do?\s+cart[aã]o\s+([a-z0-9 ]+)", t)
    if m:
        brand = _clean_trailing_tokens(m.group(1))
        if brand:
            return GROUP_EMOJI["PAG_FATURA"], f"Cartão {_titlecase(brand)}"
        return GROUP_EMOJI["PAG_FATURA"], "Cartão"
    return None, None

def _detect_saque_resgate(text: str):
    t = text.lower()
    if any(w in t for w in ["saquei", "saque", "resgatei", "resgate"]):
        origem = None
        m = re.search(r"(?:do|da|de)\s+([a-z0-9 ]+)", t)
        if m:
            origem = _clean_trailing_tokens(m.group(1))
            origem = _titlecase(origem)
        return GROUP_EMOJI["SAQUE_RESGATE"], (origem or "Saque/Resgate")
    return None, None

def detect_group_and_category(text: str) -> Tuple[str, str]:
    t = text.lower()

    grp, cat = _detect_pagamento_fatura(text)
    if grp: return grp, cat

    grp, cat = _detect_saque_resgate(text)
    if grp: return grp, cat

    for k, display in ASSINATURAS.items():
        if k in t:
            return GROUP_EMOJI["ASSINATURA"], display

    if any(w in t for w in KEYWORDS_GANHOS):
        return GROUP_EMOJI["GANHOS"], "Ganhos"

    if any(w in t for w in KEYWORDS_FIXOS):
        for w in KEYWORDS_FIXOS:
            if w in t:
                return GROUP_EMOJI["GASTOS_FIXOS"], _titlecase(w)
        return GROUP_EMOJI["GASTOS_FIXOS"], "Gasto Fixo"

    if any(w in t for w in KEYWORDS_TEMP):
        for w in KEYWORDS_TEMP:
            if w in t:
                return GROUP_EMOJI["DESPESAS_TEMP"], _titlecase(w)
        return GROUP_EMOJI["DESPESAS_TEMP"], "Despesa Temporária"

    if any(w in t for w in KEYWORDS_VARIAVEIS):
        for w in KEYWORDS_VARIAVEIS:
            if w in t:
                return GROUP_EMOJI["GASTOS_VARIAVEIS"], _titlecase(w)
        return GROUP_EMOJI["GASTOS_VARIAVEIS"], "Gasto Variável"

    if "aporte" in t or "investi" in t or "comprei ação" in t or "comprei acoes" in t:
        return GROUP_EMOJI["INVESTIMENTO"], "Aporte"
    if "reserva" in t or "meta" in t or "objetivo" in t:
        return GROUP_EMOJI["RESERVA"], "Reserva"

    return GROUP_EMOJI["GASTOS_VARIAVEIS"], "Outros"

def detect_installments(text: str) -> str:
    t = text.lower()
    m = re.search(r"(\d{1,2})x", t)
    if m: return f"{m.group(1)}x"
    if "parcelad" in t: return "parcelado"
    if "à vista" in t or "a vista" in t or "avista" in t: return "à vista"
    return "à vista"

def parse_natural(text: str) -> Tuple[Optional[List], Optional[str]]:
    valor = parse_money(text)
    if valor is None:
        return None, "Não achei o valor. Ex.: 45,90"

    data_iso = parse_date(text) or datetime.now().strftime("%Y-%m-%d")
    forma = detect_payment(text)
    cond = detect_installments(text)

    group, category = detect_group_and_category(text)
    t_low = text.lower()

    # Tipo com ícones
    if group == GROUP_EMOJI["SAQUE_RESGATE"]:
        tipo = "▲ Entrada"
    elif group == GROUP_EMOJI["GANHOS"] or re.search(r"\b(ganhei|recebi|sal[aá]rio|renda)\b", t_low):
        tipo = "▲ Entrada"
    else:
        tipo = "▼ Saída"

    # Descrição curta
    desc = None
    m = re.search(r"(comprei|paguei|gastei)\s+(.*?)(?:\s+na\s+|\s+no\s+|\s+via\s+|$)", t_low)
    if m:
        raw = m.group(2)
        raw = re.sub(r"\d{1,3}(?:\.\d{3})*(?:,\d{2})|\d+(?:\.\d{2})?", "", raw)
        raw = re.sub(r"\b(hoje|ontem|\d{1,2}/\d{1,2}(?:/\d{4})?)\b", "", raw)
        raw = raw.strip(" .,-")
        if raw and len(raw) < 60:
            desc = _titlecase(raw)

    # Pagamento de fatura → descrição vazia
    if group == GROUP_EMOJI["PAG_FATURA"]:
        desc = ""

    return [data_iso, tipo, group, category, (desc or ""), float(valor), forma, cond], None

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
        await tg_send(chat_id, "Operação cancelada. Envie /start para começar novamente.")
        return {"ok": True}

    # /start amigável
    if text.lower() == "/start":
        record_usage(chat_id, "start")
        set_pending(chat_id_str, "await_license", None)
        await tg_send(chat_id,
            "Olá! 👋\nPor favor, *informe sua licença* (ex.: `GF-ABCD-1234`).\n\n"
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

        # NEW: grava o e-mail na planilha de licenças
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
        await tg_send(chat_id, "Agora pode me contar seus gastos. Ex.: _gastei 45,90 no mercado via cartão hoje_")
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

        # NEW: atualiza a planilha de licenças usando a licença guardada em pending.temp_license
        try:
            if LICENSE_SHEET_ID and temp_license:
                sheet_update_license_email(temp_license, email)
        except Exception as e:
            logger.error(f"Falha ao atualizar e-mail da licença no Sheets: {e}")

        set_pending(chat_id_str, None, None)
        await tg_send(chat_id, "✅ Obrigado! Configurando sua planilha de lançamentos...")

        okf, errf, link = await setup_client_file(chat_id_str, email)
        if not okf:
            logger.error(f"ERRO CRÍTICO NO SETUP DO ARQUIVO: {errf}")
            await tg_send(chat_id, f"❌ Falha na configuração: {errf}. Verifique os logs do servidor.")
            return {"ok": True}

        await tg_send(chat_id, f"🚀 Planilha configurada com sucesso!\n🔗 {link}")
        await tg_send(chat_id, "Agora pode me contar seus gastos. Ex.: _gastei 45,90 no mercado via cartão hoje_")
        return {"ok": True}

    # exige licença
    ok, msg = require_active_license(chat_id_str)
    if not ok:
        await tg_send(chat_id, f"❗ {msg}")
        return {"ok": True}

    # Lançamento
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
