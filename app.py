# -*- coding: utf-8 -*-
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
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Postgres (Neon)
import psycopg2
import psycopg2.extras

# ---------------------------------------
# LOG
# ---------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("planilha-bot")

# ---------------------------------------
# FASTAPI
# ---------------------------------------
app = FastAPI()

# ---------------------------------------
# ENVs
# ---------------------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID", "").strip()
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

SQLITE_PATH = os.getenv("SQLITE_PATH", "/tmp/db.sqlite")

GOOGLE_USE_OAUTH = os.getenv("GOOGLE_USE_OAUTH", "1") == "1"
GOOGLE_OAUTH_CLIENT_ID = os.getenv("GOOGLE_OAUTH_CLIENT_ID")
GOOGLE_OAUTH_CLIENT_SECRET = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET")
GOOGLE_OAUTH_REDIRECT_URI = os.getenv("GOOGLE_OAUTH_REDIRECT_URI")
GOOGLE_OAUTH_SCOPES = (os.getenv("GOOGLE_OAUTH_SCOPES") or
                       "https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/spreadsheets").split()
GOOGLE_TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "/tmp/google_oauth_token.json")
OAUTH_STATE_SECRET = os.getenv("OAUTH_STATE_SECRET", "change-me")

GS_TEMPLATE_ID = os.getenv("GS_TEMPLATE_ID")
GS_DEST_FOLDER_ID = os.getenv("GS_DEST_FOLDER_ID")

WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Registro")  # sua aba
# Regras da sua planilha:
# Queremos escrever a partir da linha 8 e **coluna B** (ou seja, range B:I)
ROW_START = 8
COL_START_LETTER = "B"  # B até I

# SCOPES para lib de service account (fallback se você quiser)
SCOPES_SA = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]


# ---------------------------------------
# TELEGRAM
# ---------------------------------------
async def tg_send(chat_id, text):
    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN ausente")
        return
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


# ---------------------------------------
# SQLITE (mantemos uso leve para estados/pendências)
# ---------------------------------------
def _db():
    return sqlite3.connect(SQLITE_PATH)


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def local_db_init():
    con = _db()
    cur = con.cursor()
    # apenas dados transitórios do bot
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clients (
        chat_id TEXT PRIMARY KEY,
        license_key TEXT,
        email TEXT,
        file_scope TEXT,
        item_id TEXT,
        created_at TEXT,
        last_seen_at TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS usage (
        chat_id TEXT,
        event TEXT,
        ts TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pending (
        chat_id TEXT PRIMARY KEY,
        step TEXT,
        temp_license TEXT,
        created_at TEXT
    );
    """)
    con.commit()
    con.close()


def record_usage(chat_id, event):
    con = _db()
    con.execute("INSERT INTO usage(chat_id, event, ts) VALUES(?,?,?)",
                (str(chat_id), event, _now_iso()))
    con.commit()
    con.close()


def set_pending(chat_id: str, step: Optional[str], temp_license: Optional[str]):
    con = _db()
    if step:
        con.execute("""
        INSERT INTO pending(chat_id, step, temp_license, created_at)
        VALUES(?,?,?,?)
        ON CONFLICT(chat_id) DO UPDATE SET step=excluded.step,
                                          temp_license=excluded.temp_license,
                                          created_at=excluded.created_at
        """, (str(chat_id), step, temp_license, _now_iso()))
    else:
        con.execute("DELETE FROM pending WHERE chat_id=?", (str(chat_id),))
    con.commit()
    con.close()


def get_pending(chat_id: str):
    con = _db()
    cur = con.execute("SELECT step, temp_license FROM pending WHERE chat_id=?", (str(chat_id),))
    row = cur.fetchone()
    con.close()
    return (row[0], row[1]) if row else (None, None)


def set_client_email(chat_id: str, email: str):
    con = _db()
    con.execute("INSERT INTO clients(chat_id, created_at) VALUES(?,?) ON CONFLICT(chat_id) DO NOTHING",
                (str(chat_id), _now_iso()))
    con.execute("UPDATE clients SET email=?, last_seen_at=? WHERE chat_id=?",
                (email, _now_iso(), str(chat_id)))
    con.commit()
    con.close()


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


def set_client_file(chat_id: str, item_id: str):
    con = _db()
    con.execute("INSERT INTO clients(chat_id, created_at) VALUES(?,?) ON CONFLICT(chat_id) DO NOTHING",
                (str(chat_id), _now_iso()))
    con.execute("""UPDATE clients SET file_scope=?, item_id=?, last_seen_at=?
                   WHERE chat_id=?""",
                ("google", item_id, _now_iso(), str(chat_id)))
    con.commit()
    con.close()


def set_client_license(chat_id: str, license_key: str):
    con = _db()
    con.execute("INSERT INTO clients(chat_id, created_at) VALUES(?,?) ON CONFLICT(chat_id) DO NOTHING",
                (str(chat_id), _now_iso()))
    con.execute("UPDATE clients SET license_key=?, last_seen_at=? WHERE chat_id=?",
                (license_key, _now_iso(), str(chat_id)))
    con.commit()
    con.close()


# ---------------------------------------
# POSTGRES (Neon) - Licenças
# ---------------------------------------
def pg_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL ausente (Neon)")
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def pg_init():
    """Cria tabela de licenças se não existir (colunas de acordo com o que você montou no Neon)."""
    with pg_conn() as con, con.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS licenses(
            id SERIAL PRIMARY KEY,
            chave_de_licença TEXT UNIQUE NOT NULL,
            email TEXT,
            data_de_início TIMESTAMP,
            data_final TIMESTAMP,
            dias_de_validade INTEGER NOT NULL DEFAULT 30,
            status TEXT NOT NULL DEFAULT 'ativo',
            notas TEXT
        );
        """)
        con.commit()


def pg_get_license(license_key: str) -> Optional[dict]:
    with pg_conn() as con, con.cursor() as cur:
        cur.execute("SELECT * FROM licenses WHERE chave_de_licença=%s LIMIT 1", (license_key,))
        row = cur.fetchone()
        return dict(row) if row else None


def pg_update_license_email_and_dates(license_key: str, email: str):
    """Se a licença ainda não tem data de início/final, marca agora.
       Sempre atualiza e-mail (último e-mail válido do cliente)."""
    now = datetime.now(timezone.utc)
    with pg_conn() as con, con.cursor() as cur:
        # pega linha atual
        cur.execute("SELECT * FROM licenses WHERE chave_de_licença=%s LIMIT 1", (license_key,))
        row = cur.fetchone()
        if not row:
            return

        start = row["data_de_início"]
        end = row["data_final"]
        days = row.get("dias_de_validade", 30) or 30

        if not start:
            start = now
            end = now + timedelta(days=days)

        cur.execute("""
            UPDATE licenses
               SET email=%s,
                   data_de_início=%s,
                   data_final=%s
             WHERE chave_de_licença=%s
        """, (email, start, end, license_key))
        con.commit()


def pg_set_status(license_key: str, new_status: str):
    with pg_conn() as con, con.cursor() as cur:
        cur.execute("UPDATE licenses SET status=%s WHERE chave_de_licença=%s", (new_status, license_key))
        con.commit()


def check_license_allowed(license_key: str) -> Tuple[bool, str]:
    """Valida na tabela do Neon: status, expiração."""
    lic = pg_get_license(license_key)
    if not lic:
        return False, "Licença não encontrada."

    status = (lic.get("status") or "").lower().strip()
    if status != "ativo":
        return False, "Licença desativada."

    end = lic.get("data_final")
    if end:
        now = datetime.now(timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        if now > end:
            return False, f"Licença expirada em {end.strftime('%Y-%m-%d %H:%M')}."
    return True, ""


# ---------------------------------------
# GOOGLE AUTH & SERVICES
# ---------------------------------------
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


def google_services():
    """Only OAuth (sua conta) — recomendado."""
    from google.auth.transport.requests import Request
    creds = _load_credentials()
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            _save_credentials(creds)
        else:
            raise RuntimeError("Autorize primeiro em /oauth/start")
    drive = build("drive", "v3", credentials=creds)
    sheets = build("sheets", "v4", credentials=creds)
    return drive, sheets


# ---------------------------------------
# GOOGLE DRIVE
# ---------------------------------------
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
        s = str(e)
        if "already has permission" not in s and "Domain policy" not in s:
            logger.error(f"Erro ao compartilhar {file_id} com {email}: {e}")
            raise
    meta = drive.files().get(fileId=file_id, fields="webViewLink").execute()
    return meta.get("webViewLink")


def drive_find_in_folder(service, folder_id: str, name: str) -> Optional[str]:
    safe_name = name.replace("'", "\\'")
    q = f"name = '{safe_name}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=1).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None


# ---------------------------------------
# GOOGLE SHEETS  (escrever a partir da B8 sem inserir linhas)
# ---------------------------------------
def sheets_find_first_empty_row_from(sheets, spreadsheet_id: str, sheet_name: str, start_row: int) -> int:
    # Lê a coluna B (onde há a "Data") a partir da linha 8
    rng = f"{sheet_name}!B{start_row}:B"
    res = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=rng, majorDimension="ROWS"
    ).execute()
    values = res.get("values", [])
    # A primeira linha vazia é start_row + len(valores_não_vazios)
    for idx, row in enumerate(values, 0):
        if not row or not (row[0] or "").strip():
            return start_row + idx
    return start_row + len(values)


def sheets_write_row(spreadsheet_id: str, sheet_name: str, values: List):
    """Escreve na primeira linha vazia começando em B8 (B..I).
       Colunas: Data, Tipo, Grupo, Categoria, Descrição, Valor, Forma, Condição"""
    _, sheets = google_services()
    first_row = sheets_find_first_empty_row_from(sheets, spreadsheet_id, sheet_name, ROW_START)
    rng = f"{sheet_name}!B{first_row}:I{first_row}"
    body = {"values": [values]}
    sheets.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=rng,
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()


# ---------------------------------------
# NLP / Parsing (melhorias)
# ---------------------------------------
def parse_money(text: str) -> Optional[float]:
    m = re.search(r"(\d{1,3}(?:\.\d{3})*(?:,\d{2})|\d+(?:\.\d{2})?)", text)
    if not m:
        return None
    val = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(val)
    except:
        return None


def parse_date(text: str) -> Optional[str]:
    t = text.lower()
    today = datetime.now().date()
    if "hoje" in t:
        return today.strftime("%Y-%m-%d")
    if "ontem" in t:
        return (today - timedelta(days=1)).strftime("%Y-%m-%d")
    m = re.search(r"\b(\d{1,2})/(\d{1,2})(?:/(\d{4}))?\b", text)
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3) or str(today.year)
        try:
            dt = datetime.strptime(f"{d}/{mo}/{y}", "%d/%m/%Y").date()
            return dt.strftime("%Y-%m-%d")
        except:
            return None
    return None


# Grupos com ícones conforme seu pedido
GROUPS = {
    "🏠Gastos Fixos": ["aluguel", "condomínio", "agua", "água", "energia", "internet", "plano de saúde", "escola"],
    "📺Assinatura": ["netflix", "amazon", "disney", "spotify", "premiere", "globoplay", "apple tv"],
    "💸Gastos Variáveis": ["mercado", "supermercado", "farmácia", "combustível", "passeio", "ifood", "viagem", "restaurante", "lanche", "pizza", "hamburg", "sushi"],
    "🧾Despesas Temporárias": ["iptu", "ipva", "financiamento", "empréstimo"],
    "💳Pagamento de Fatura": ["fatura", "cartão", "fatura do cartão"],
    "💵Ganhos": ["salário", "vale", "renda extra", "pro labore", "pró labore"],
    "💰Investimento": ["renda fixa", "renda variável", "fundos imobiliários", "tesouro"],
    "📝Reserva": ["trocar de carro", "viagem pra disney", "reserva"],
    "💲Saque/Resgate": ["saquei", "saque", "resgatei", "resgate"],
}

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def detect_group_and_category(text: str) -> Tuple[str, str]:
    t = text.lower()
    for group, kws in GROUPS.items():
        for kw in kws:
            if kw in t:
                if group == "💳Pagamento de Fatura":
                    # Categoria vira o nome do cartão
                    m = re.search(r"cart[aã]o\s+([a-z0-9 ]+)", t)
                    cat = f"Cartão {normalize_spaces(m.group(1))}" if m else "Cartão"
                    return group, cat.title()
                return group, kw.title()
    # Default
    return "💸Gastos Variáveis", "Outros"


def detect_payment(text: str) -> str:
    t = text.lower()
    # Não capturar "hoje" como parte do cartão
    m = re.search(r"cart[aã]o\s+([a-z0-9 ]+?)(?:\s+hoje\b|\s|$)", t)
    if m:
        brand = normalize_spaces(m.group(1).replace("hoje", ""))
        brand = re.sub(r"\bhoje\b", "", brand, flags=re.I)
        brand = normalize_spaces(brand)
        return f"💳 cartão {brand}" if brand else "💳 cartão"

    if "pix" in t:
        return "Pix"
    if "dinheiro" in t or "cash" in t:
        return "Dinheiro"
    if "débito" in t or "debito" in t:
        return "Débito"
    if "crédito" in t or "credito" in t:
        return "💳 cartão"
    return "Outros"


def detect_installments(text: str) -> str:
    t = text.lower()
    m = re.search(r"(\d{1,2})x", t)
    if m:
        return f"{m.group(1)}x"
    if "parcelad" in t:
        return "parcelado"
    if "à vista" in t or "a vista" in t or "avista" in t:
        return "à vista"
    return "à vista"


def parse_natural(text: str) -> Tuple[Optional[List], Optional[str]]:
    valor = parse_money(text)
    if valor is None:
        return None, "Não achei o valor. Ex.: 45,90"

    data_iso = parse_date(text) or datetime.now().strftime("%Y-%m-%d")
    group, category = detect_group_and_category(text)
    forma = detect_payment(text)
    cond = detect_installments(text)

    # ▲ Entrada / ▼ Saída
    tipo = "▲ Entrada" if re.search(r"\b(ganhei|recebi|sal[aá]rio|renda)\b", text.lower()) else "▼ Saída"

    # Descrição: tente extrair qualquer coisa útil sem data/valor
    desc = ""
    m = re.search(r"(comprei|paguei|gastei)\s+(.*?)(?:\s+na\s+|\s+no\s+|\s+via\s+|$)", text.lower())
    if m:
        raw = m.group(2)
        raw = re.sub(r"\d{1,3}(?:\.\d{3})*(?:,\d{2})|\d+(?:\.\d{2})?", "", raw)  # remove valores
        raw = re.sub(r"\b(hoje|ontem|\d{1,2}/\d{1,2}(?:/\d{4})?)\b", "", raw)  # remove datas
        desc = normalize_spaces(raw)[:60]

    # ordem (B..I): Data, Tipo, Grupo, Categoria, Descrição, Valor, Forma, Condição
    row = [data_iso, tipo, group, category, desc, float(valor), forma, cond]
    return row, None


# ---------------------------------------
# PROVISIONAMENTO / CLIENT
# ---------------------------------------
def ensure_unique_or_reuse(email: str) -> Optional[str]:
    if not GS_DEST_FOLDER_ID:
        return None
    drive, _ = google_services()
    name = f"Lancamentos - {email}"
    return drive_find_in_folder(drive, GS_DEST_FOLDER_ID, name)


def drive_copy_and_link(email: str) -> Tuple[str, str]:
    new_name = f"Lancamentos - {email}"
    file_id = drive_copy_template(new_name)
    link = drive_share_with_email(file_id, email, role="writer")
    return file_id, link


async def setup_client_file(chat_id: str, email: str) -> Tuple[bool, Optional[str], Optional[str]]:
    cli = get_client(chat_id)
    if cli and cli.get("item_id"):
        try:
            link = drive_share_with_email(cli["item_id"], email, role="writer")
        except Exception:
            link = None
        return True, None, link

    try:
        exist = ensure_unique_or_reuse(email)
        if exist:
            set_client_file(str(chat_id), exist)
            try:
                link = drive_share_with_email(exist, email, role="writer")
            except Exception:
                link = None
            return True, None, link

        new_id, web_link = drive_copy_and_link(email)
        set_client_file(str(chat_id), new_id)
        return True, None, web_link
    except HttpError as e:
        logger.error(f"HttpError Google: {e}")
        return False, f"Falha Google API: {e}", None
    except Exception as e:
        logger.error(f"setup_client_file exception: {e}")
        return False, f"Falha ao criar planilha: {e}", None


def add_row_to_client(values: List, chat_id: str):
    if len(values) != 8:
        raise RuntimeError(f"Esperava 8 colunas, recebi {len(values)}.")
    cli = get_client(chat_id)
    if not cli or not cli.get("item_id"):
        raise RuntimeError("Planilha do cliente não configurada.")
    sheets_write_row(cli["item_id"], WORKSHEET_NAME, values)


# ---------------------------------------
# LICENÇA (Regra de acesso)
# ---------------------------------------
def require_active_license(chat_id: str) -> Tuple[bool, str]:
    cli = get_client(chat_id)
    if not cli or not cli.get("license_key"):
        return False, "Para usar o bot você precisa **ativar sua licença**. Envie /start e siga as instruções."
    ok, err = check_license_allowed(cli["license_key"])
    if not ok:
        return False, f"Licença inválida: {err}\nFale com o suporte para renovar/ativar."
    return True, ""


# ---------------------------------------
# ROUTES
# ---------------------------------------
@app.on_event("startup")
def _startup():
    local_db_init()
    pg_init()
    logger.info("✅ Startup ok")
    logger.info(f"Auth mode: {'OAuth' if GOOGLE_USE_OAUTH else 'Service Account'}")


@app.get("/")
def root():
    return {"status": "ok", "auth_mode": "oauth" if GOOGLE_USE_OAUTH else "sa"}


@app.get("/ping")
def ping():
    return {"pong": True}


# ---------- OAuth ----------
@app.get("/oauth/start")
def oauth_start():
    if not GOOGLE_USE_OAUTH:
        return HTMLResponse("<h3>OAuth desabilitado. Defina GOOGLE_USE_OAUTH=1.</h3>", status_code=400)
    if not (GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET and GOOGLE_OAUTH_REDIRECT_URI):
        return HTMLResponse("<h3>Faltam variáveis do OAuth no ambiente.</h3>", status_code=500)
    flow = Flow.from_client_config(_client_config_dict(), scopes=GOOGLE_OAUTH_SCOPES,
                                   redirect_uri=GOOGLE_OAUTH_REDIRECT_URI)
    auth_url, state = flow.authorization_url(access_type="offline", include_granted_scopes="true",
                                             prompt="consent", state=OAUTH_STATE_SECRET)
    return RedirectResponse(auth_url)


@app.get("/oauth/callback")
@app.get("/oauth2callback")
def oauth_callback(code: str | None = None, state: str | None = None):
    if not GOOGLE_USE_OAUTH:
        return HTMLResponse("<h3>OAuth desabilitado. Defina GOOGLE_USE_OAUTH=1.</h3>", status_code=400)
    if state != OAUTH_STATE_SECRET:
        return HTMLResponse("<h3>State inválido.</h3>", status_code=400)
    if not code:
        return HTMLResponse("<h3>Faltou 'code'.</h3>", status_code=400)
    flow = Flow.from_client_config(_client_config_dict(), scopes=GOOGLE_OAUTH_SCOPES,
                                   redirect_uri=GOOGLE_OAUTH_REDIRECT_URI)
    flow.fetch_token(code=code)
    creds = flow.credentials
    if not creds.refresh_token:
        return HTMLResponse("<h3>Não veio refresh_token. Refazer /oauth/start.</h3>", status_code=400)
    _save_credentials(creds)
    return HTMLResponse("<h3>✅ OAuth ok! Pode voltar ao Telegram.</h3>")


# ---------- Telegram webhook ----------
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

    # ADMIN
    if ADMIN_TELEGRAM_ID and chat_id_str == ADMIN_TELEGRAM_ID:
        if text.lower().startswith("/licenca desativar"):
            parts = text.split(maxsplit=2)
            if len(parts) >= 3:
                key = parts[2].strip()
                pg_set_status(key, "inativo")
                await tg_send(chat_id, f"🔒 Licença `{key}` desativada.")
            else:
                await tg_send(chat_id, "Use: /licenca desativar GF-XXXX-XXXX")
            return {"ok": True}
        if text.lower().startswith("/licenca ativar"):
            parts = text.split(maxsplit=2)
            if len(parts) >= 3:
                key = parts[2].strip()
                pg_set_status(key, "ativo")
                await tg_send(chat_id, f"✅ Licença `{key}` ativada.")
            else:
                await tg_send(chat_id, "Use: /licenca ativar GF-XXXX-XXXX")
            return {"ok": True}

    # /cancel
    if text.lower() == "/cancel":
        set_pending(chat_id_str, None, None)
        await tg_send(chat_id, "Operação cancelada. Envie /start para começar novamente.")
        return {"ok": True}

    # /start (novo fluxo)
    if text.lower() == "/start":
        record_usage(chat_id, "start")
        set_pending(chat_id_str, "await_license", None)
        await tg_send(chat_id,
            "Olá! 👋\nPor favor, *informe sua licença* (ex.: `GF-ABCD-1234`).\n\n"
            "Você pode digitar /cancel para cancelar."
        )
        return {"ok": True}

    # /start TOKEN [email] (atalho)
    if text.lower().startswith("/start "):
        record_usage(chat_id, "start_token")
        parts = text.split()
        token = parts[1].strip() if len(parts) >= 2 else None
        email = parts[2].strip() if len(parts) >= 3 else None

        if not token:
            await tg_send(chat_id, "Envie `/start SEU-CÓDIGO` (ex.: `/start GF-ABCD-1234`).")
            return {"ok": True}

        ok, err = check_license_allowed(token)
        if not ok:
            await tg_send(chat_id, f"❌ Licença inválida: {err}")
            return {"ok": True}

        set_client_license(chat_id_str, token)

        if not email:
            set_pending(chat_id_str, "await_email", token)
            await tg_send(chat_id, "Licença ok ✅\nAgora me diga seu *e-mail* (ex.: `cliente@gmail.com`).")
            return {"ok": True}

        set_client_email(chat_id_str, email)
        pg_update_license_email_and_dates(token, email)

        await tg_send(chat_id, "✅ Obrigado! Configurando sua planilha de lançamentos...")
        okf, errf, link = await setup_client_file(chat_id_str, email)
        if not okf:
            logger.error(f"ERRO CRÍTICO NO SETUP DO ARQUIVO: {errf}")
            await tg_send(chat_id, f"❌ Falha na configuração: {errf}.")
            return {"ok": True}
        await tg_send(chat_id, f"🚀 Planilha configurada com sucesso!\n🔗 {link}")
        await tg_send(chat_id, "Agora pode me contar seus gastos. Ex.: _gastei 45,90 no mercado via cartão hoje_")
        return {"ok": True}

    # Conversa pendente
    step, temp_license = get_pending(chat_id_str)
    if step == "await_license":
        token = text.strip()
        ok, err = check_license_allowed(token)
        if not ok:
            await tg_send(chat_id, f"❌ Licença inválida: {err}\nTente novamente ou digite /cancel.")
            return {"ok": True}
        set_client_license(chat_id_str, token)
        set_pending(chat_id_str, "await_email", token)
        await tg_send(chat_id, "Licença ok ✅\nAgora me diga seu *e-mail* (ex.: `cliente@gmail.com`).")
        return {"ok": True}

    if step == "await_email":
        email = text.strip()
        if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
            await tg_send(chat_id, "❗ E-mail inválido. Tente novamente (ex.: `cliente@gmail.com`).")
            return {"ok": True}

        # amarra no cliente
        set_client_email(chat_id_str, email)
        # registra email e datas na licença
        lic_key = temp_license or (get_client(chat_id_str) or {}).get("license_key")
        if lic_key:
            pg_update_license_email_and_dates(lic_key, email)

        set_pending(chat_id_str, None, None)
        await tg_send(chat_id, "✅ Obrigado! Configurando sua planilha de lançamentos...")

        okf, errf, link = await setup_client_file(chat_id_str, email)
        if not okf:
            logger.error(f"ERRO CRÍTICO NO SETUP DO ARQUIVO: {errf}")
            await tg_send(chat_id, f"❌ Falha na configuração: {errf}.")
            return {"ok": True}

        await tg_send(chat_id, f"🚀 Planilha configurada com sucesso!\n🔗 {link}")
        await tg_send(chat_id, "Agora pode me contar seus gastos. Ex.: _gastei 45,90 no mercado via cartão hoje_")
        return {"ok": True}

    # exige licença válida
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
