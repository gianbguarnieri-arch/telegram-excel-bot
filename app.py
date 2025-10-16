import os
import re
import json
import secrets
import string
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List

import httpx
from fastapi import FastAPI, Request, Header
from fastapi.responses import HTMLResponse, RedirectResponse

# Google APIs (inalterado)
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ====== PostgreSQL ======
import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# =========================================================
# ENVs (Telegram + Google)
# =========================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()

# >>> NOVO: Postgres (Neon)
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não configurado. Defina essa variável no Render.")

# ====== Google: modos de autenticação ======
GOOGLE_USE_OAUTH = os.getenv("GOOGLE_USE_OAUTH", "0") == "1"
GOOGLE_OAUTH_CLIENT_ID     = os.getenv("GOOGLE_OAUTH_CLIENT_ID")
GOOGLE_OAUTH_CLIENT_SECRET = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET")
GOOGLE_OAUTH_REDIRECT_URI  = os.getenv("GOOGLE_OAUTH_REDIRECT_URI")
GOOGLE_OAUTH_SCOPES = (os.getenv("GOOGLE_OAUTH_SCOPES") or
                       "https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/spreadsheets").split()
GOOGLE_TOKEN_PATH  = os.getenv("GOOGLE_TOKEN_PATH", "/tmp/google_oauth_token.json")
OAUTH_STATE_SECRET = os.getenv("OAUTH_STATE_SECRET", "change-me")

# (2) Service Account — fallback
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")

# ====== IDs do modelo e pasta de destino (Drive) ======
GS_TEMPLATE_ID    = os.getenv("GS_TEMPLATE_ID")
GS_DEST_FOLDER_ID = os.getenv("GS_DEST_FOLDER_ID")

# Opções
WORKSHEET_NAME   = os.getenv("WORKSHEET_NAME", "Plan1")  # você pode pôr "🧾" aqui
SHARE_LINK_ROLE  = os.getenv("SHARE_LINK_ROLE", "writer")  # writer|commenter|reader

SCOPES_SA = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# =========================================================
# DB helpers (PostgreSQL)
# =========================================================
def pg_conn():
    # autocommit=True simplifica updates/insert sem precisar chamar commit em todo lugar
    conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    conn.autocommit = True
    return conn

def licenses_db_init():
    """Cria o schema necessário no Postgres (idempotente)."""
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS licenses (
            license_key   TEXT PRIMARY KEY,
            status        TEXT NOT NULL DEFAULT 'active',
            max_files     INTEGER NOT NULL DEFAULT 1,
            expires_at    TIMESTAMPTZ,
            notes         TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS clients (
            chat_id     TEXT PRIMARY KEY,
            license_key TEXT REFERENCES licenses(license_key),
            email       TEXT,
            file_scope  TEXT,
            item_id     TEXT,
            created_at  TIMESTAMPTZ,
            last_seen_at TIMESTAMPTZ
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS usage_log (
            chat_id TEXT,
            event   TEXT,
            ts      TIMESTAMPTZ
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS pending (
            chat_id      TEXT PRIMARY KEY,
            step         TEXT,
            temp_license TEXT,
            created_at   TIMESTAMPTZ
        );
        """
    ]
    with pg_conn() as conn:
        with conn.cursor() as cur:
            for q in ddl:
                cur.execute(q)
    logger.info("✅ Postgres schema pronto.")

def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def record_usage(chat_id, event):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO usage_log(chat_id, event, ts) VALUES (%s,%s, %s)",
                (str(chat_id), event, datetime.now(timezone.utc))
            )

def _gen_key(prefix="GF"):
    alphabet = string.ascii_uppercase + string.digits
    part = lambda n: "".join(secrets.choice(alphabet) for _ in range(n))
    return f"{prefix}-{part(4)}-{part(4)}"

def create_license(days: Optional[int] = 30, max_files: int = 1, notes: Optional[str] = None, custom_key: Optional[str] = None):
    key = custom_key or _gen_key()
    expires_at = (datetime.now(timezone.utc) + timedelta(days=days)) if days else None
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO licenses(license_key,status,max_files,expires_at,notes) VALUES(%s,%s,%s,%s,%s)",
                (key, "active", max_files, expires_at, notes)
            )
    return key, expires_at.isoformat(timespec="seconds") if expires_at else None

def get_license(license_key: str):
    with pg_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT license_key,status,max_files,expires_at,notes FROM licenses WHERE license_key=%s",
                (license_key,)
            )
            row = cur.fetchone()
    if not row:
        return None
    return {
        "license_key": row["license_key"],
        "status": row["status"],
        "max_files": row["max_files"],
        "expires_at": row["expires_at"].isoformat(timespec="seconds") if row["expires_at"] else None,
        "notes": row["notes"]
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
    with pg_conn() as conn:
        with conn.cursor() as cur:
            # Verifica se essa licença já está vinculada a outro chat
            cur.execute("SELECT chat_id FROM clients WHERE license_key=%s AND chat_id<>%s LIMIT 1",
                        (license_key, str(chat_id)))
            conflict = cur.fetchone()
            if conflict:
                return False, "Essa licença já foi usada por outro Telegram."
            # upsert do cliente
            cur.execute(
                """
                INSERT INTO clients(chat_id, created_at, last_seen_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (chat_id) DO NOTHING
                """,
                (str(chat_id), datetime.now(timezone.utc), datetime.now(timezone.utc))
            )
            cur.execute(
                "UPDATE clients SET license_key=%s, last_seen_at=%s WHERE chat_id=%s",
                (license_key, datetime.now(timezone.utc), str(chat_id))
            )
    return True, None

def get_client(chat_id: str):
    with pg_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """SELECT chat_id, license_key, email, file_scope, item_id, created_at, last_seen_at
                   FROM clients WHERE chat_id=%s""",
                (str(chat_id),)
            )
            row = cur.fetchone()
    if not row:
        return None
    return {
        "chat_id": row["chat_id"],
        "license_key": row["license_key"],
        "email": row["email"],
        "file_scope": row["file_scope"],
        "item_id": row["item_id"],
        "created_at": row["created_at"].isoformat(timespec="seconds") if row["created_at"] else None,
        "last_seen_at": row["last_seen_at"].isoformat(timespec="seconds") if row["last_seen_at"] else None,
    }

def set_client_email(chat_id: str, email: str):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE clients SET email=%s, last_seen_at=%s WHERE chat_id=%s",
                (email, datetime.now(timezone.utc), str(chat_id))
            )

def set_client_file(chat_id: str, item_id: str):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE clients SET file_scope=%s, item_id=%s, last_seen_at=%s WHERE chat_id=%s",
                ("google", item_id, datetime.now(timezone.utc), str(chat_id))
            )

def set_pending(chat_id: str, step: Optional[str], temp_license: Optional[str]):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            if step:
                cur.execute(
                    """
                    INSERT INTO pending(chat_id, step, temp_license, created_at)
                    VALUES(%s,%s,%s,%s)
                    ON CONFLICT (chat_id) DO UPDATE SET
                        step=EXCLUDED.step,
                        temp_license=EXCLUDED.temp_license,
                        created_at=EXCLUDED.created_at
                    """,
                    (str(chat_id), step, temp_license, datetime.now(timezone.utc))
                )
            else:
                cur.execute("DELETE FROM pending WHERE chat_id=%s", (str(chat_id),))

def get_pending(chat_id: str):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT step, temp_license FROM pending WHERE chat_id=%s", (str(chat_id),))
            row = cur.fetchone()
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


# =========================================================
# Telegram helper
# =========================================================
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

# =========================================================
# Google helpers — Auth (inalterado)
# =========================================================
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
    return _oauth_services() if GOOGLE_USE_OAUTH else _sa_services()

# =========================================================
# Google helpers — Drive/Sheets (inalterado)
# =========================================================
def drive_find_in_folder(service, folder_id: str, name: str) -> Optional[str]:
    safe_name = name.replace("'", "\\'")
    q = f"name = '{safe_name}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=1).execute()
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
    _, sheets = google_services()
    body = {"values": [values]}
    # Mantém o append (Sheets decide a próxima linha dentro do range da planilha)
    rng = f"{sheet_name}!A:H"  # ajuste aqui se sua planilha começar em outra coluna/aba
    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=rng,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()

# =========================================================
# NLP e Parsers (igual ao que você já usava)
# =========================================================
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

def detect_payment(text: str) -> str:
    t = text.lower()
    m = re.search(r"cart[aã]o\s+([a-z0-9 ]+)", t)
    if m:
        brand = m.group(1).strip()
        brand = re.sub(r"\s+", " ", brand).strip()
        # remove "hoje"/"ontem" acidental no nome do cartão
        brand = re.sub(r"\b(hoje|ontem)\b", "", brand).strip()
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

CATEGORIES = {
    "Restaurante": ["restaurante", "almoço", "jantar", "lanche", "pizza", "hamburg", "sushi"],
    "Mercado": ["mercado", "supermercado", "compras de mercado", "rancho", "hortifruti"],
    "Farmácia": ["farmácia", "remédio", "medicamento", "drogaria"],
    "Combustível": ["gasolina", "álcool", "etanol", "diesel", "posto", "combustível"],
    "Ifood": ["ifood", "i-food"],
    "Passeio em família": ["passeio", "parque", "cinema", "lazer"],
    "Viagem": ["hotel", "passagem", "viagem", "airbnb"],
    "Assinatura": ["netflix", "amazon", "disney", "spotify", "premiere"],
    "Aluguel": ["aluguel", "condomínio"],
    "Água": ["água", "sabesp"],
    "Energia": ["energia", "luz"],
    "Internet": ["internet", "banda larga", "fibra"],
    "Plano de Saúde": ["plano de saúde", "unimed", "amil"],
    "Escola": ["escola", "mensalidade", "faculdade", "curso"],
    "Imposto": ["iptu", "ipva"],
    "Financiamento": ["financiamento", "parcela do carro", "parcela da casa"],
}

def map_group(category: str) -> str:
    if category in ["Aluguel", "Água", "Energia", "Internet", "Plano de Saúde", "Escola", "Assinatura"]:
        return "Gastos Fixos"
    if category in ["Imposto", "Financiamento", "Empréstimo"]:
        return "Despesas Temporárias"
    if category in ["Mercado", "Farmácia", "Combustível", "Passeio em família", "Ifood", "Viagem", "Restaurante"]:
        return "Gastos Variáveis"
    if category in ["Salário", "Vale", "Renda Extra 1", "Renda Extra 2", "Pró labore"]:
        return "Ganhos"
    if category in ["Renda Fixa", "Renda Variável", "Fundos imobiliários"]:
        return "Investimento"
    if category in ["Trocar de carro", "Viagem pra Disney"]:
        return "Reserva"
    return "Gastos Variáveis"

def detect_category_and_desc(text: str):
    t = text.lower()
    for cat, kws in CATEGORIES.items():
        for kw in kws:
            if kw in t:
                m = re.search(r"(comprei|paguei|gastei)\s+(.*?)(?:\s+na\s+|\s+no\s+|\s+via\s+|$)", t)
                desc = None
                if m:
                    raw = m.group(2)
                    raw = re.sub(r"\d{1,3}(?:\.\d{3})*(?:,\d{2})|\d+(?:\.\d{2})?", "", raw)
                    raw = re.sub(r"\b(hoje|ontem|\d{1,2}/\d{1,2}(?:/\d{4})?)\b", "", raw)
                    raw = raw.strip(" .,-")
                    if raw and len(raw) < 60:
                        desc = raw
                return cat, (desc if desc else None)
    return "Outros", None

def parse_natural(text: str) -> Tuple[Optional[List], Optional[str]]:
    valor = parse_money(text)
    if valor is None:
        return None, "Não achei o valor. Ex.: 45,90"
    data_iso = parse_date(text) or datetime.now().strftime("%Y-%m-%d")
    forma = detect_payment(text)
    cond = detect_installments(text)
    cat, desc = detect_category_and_desc(text)
    tipo = "▲ Entrada" if re.search(r"\b(ganhei|recebi|sal[aá]rio|renda)\b", text.lower()) else "▼ Saída"
    grupo = map_group(cat)
    return [data_iso, tipo, grupo, cat, (desc or ""), float(valor), forma, cond], None

# =========================================================
# Provisionamento por cliente
# =========================================================
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

# =========================================================
# Rotas
# =========================================================
@app.on_event("startup")
def _startup():
    licenses_db_init()
    logger.info("✅ DB inicializado (Postgres).")
    print(f"Auth mode: {'OAuth' if GOOGLE_USE_OAUTH else 'Service Account'}")

@app.get("/")
def root():
    return {"status": "ok", "auth_mode": "oauth" if GOOGLE_USE_OAUTH else "sa"}

@app.get("/ping")
def ping():
    return {"pong": True}

# ===== Fluxo OAuth =====
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
def oauth_callback(code: Optional[str] = None, state: Optional[str] = None):
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

# ===== Telegram Bot Webhook =====
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

    # ===== Admin =====
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

    # ========= /cancel =========
    if text.lower() == "/cancel":
        set_pending(chat_id_str, None, None)
        await tg_send(chat_id, "Operação cancelada. Envie /start para começar novamente.")
        return {"ok": True}

    # ========= /start =========
    if text.lower() == "/start":
        record_usage(chat_id, "start")
        set_pending(chat_id_str, "await_license", None)
        await tg_send(chat_id,
            "Olá! 👋\nPor favor, *informe sua licença* (ex.: `GF-ABCD-1234`).\n\n"
            "Você pode digitar /cancel para cancelar."
        )
        return {"ok": True}

    # ========= /start TOKEN [email] =========
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
        await tg_send(chat_id, "✅ Obrigado! Configurando sua planilha de lançamentos...")
        
        okf, errf, link = await setup_client_file(chat_id_str, email)
        if not okf:
            logger.error(f"ERRO CRÍTICO NO SETUP DO ARQUIVO: {errf}")
            await tg_send(chat_id, f"❌ Falha na configuração: {errf}. Verifique os logs do servidor.")
            return {"ok": True}

        await tg_send(chat_id, f"🚀 Planilha configurada com sucesso!\n🔗 {link}")
        await tg_send(chat_id, "Agora pode me contar seus gastos. Ex.: _gastei 45,90 no mercado via cartão hoje_")
        return {"ok": True}

    # ========= Conversa pendente =========
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

    # ===== exige licença =====
    ok, msg = require_active_license(chat_id_str)
    if not ok:
        await tg_send(chat_id, f"❗ {msg}")
        return {"ok": True}

    # ===== Lançamento =====
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
