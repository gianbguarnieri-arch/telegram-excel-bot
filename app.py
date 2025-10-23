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
            rows.append(row); row = []
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
    con.commit(); con.close()

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
    con.commit(); con.close()

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
# (garante fuso local caso Parte 1 não tenha APP_TZ/_local_today definidos)
try:
    APP_TZ
except NameError:
    APP_TZ = os.getenv("APP_TZ", "America/Sao_Paulo")
try:
    _local_today
except NameError:
    from zoneinfo import ZoneInfo
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
    """
    'hoje' / 'ontem' com fuso local e dd/mm[/aa|aaaa]
    """
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
    """
    Extrai o último número, ignorando datas; normaliza '12 x'→'12x' mas usa apenas número monetário.
    """
    t = text.lower().replace("r$", " ").replace("reais", " ")
    t = re.sub(r"\b\d{1,2}[\/\-.]\d{1,2}(?:[\/\-.]\d{2,4})?\b", " ", t)  # remove datas
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
    """
    Forma de pagamento:
      - "cartão X" → "💳cartão X" (limpa 'hoje', 'via' etc. do final)
      - Pix / débito / crédito / Outros
    """
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
    CONDIÇÃO de pagamento:
      - "à vista" se houver "à vista", "a vista" ou "avista"
      - "Nx" quando houver padrão de parcelamento, com ou sem espaço antes do 'x':
          "em 10x", "10x", "parcelado em 12x", "21 x", "em 3 x", "parcelada 4 x"
      - Sempre normaliza para "Nx" (sem espaço), ex.: "12 x" -> "12x"
    """
    t = text.lower()

    # à vista explícito
    if re.search(r"\b(a\s+vista|à\s+vista|avista)\b", t):
        return "à vista"

    # procura quantidade de parcelas (1–2 dígitos) seguido de 'x', com/sem espaço e com/sem "parcelado/em"
    m = re.search(r"(?:parcelad[oa]\s*(?:em)?\s*|em\s*)?(\d{1,2})\s*x\b", t)
    if m:
        n = int(m.group(1))
        return f"{n}x"

    # nenhum indicativo de parcelamento
    return "à vista"

def _category_before_comma(text: str) -> Optional[str]:
    """
    Tudo antes da primeira vírgula é a categoria.
    """
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
# Mapeamento visual dos grupos (sem espaço após emoji)
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

# ===========================
# NLP (modo texto livre)
# ===========================
def detect_group_and_category_free(text: str) -> Tuple[str, str]:
    t = text.lower()

    # Saque / Resgate
    if any(w in t for w in ["saquei", "saque ", "resgatei", "resgate "]):
        cat = _category_before_comma(text) or "Saque/Resgate"
        return GROUP_EMOJI["SAQUE_RESGATE"], cat

    # Reserva
    if "reservei" in t or "reserva" in t:
        cat = _category_before_comma(text) or "Reserva"
        return GROUP_EMOJI["RESERVA"], cat

    # Investimento
    if ("investi" in t) or ("investimento" in t):
        cat = _category_before_comma(text)
        if not cat:
            if "renda fixa" in t: cat = "Renda Fixa"
            elif "aç" in t or "aco" in t or "ações" in t or "acoes" in t: cat = "Ações"
            else: cat = "Investimento"
        return GROUP_EMOJI["INVESTIMENTO"], cat

    # Pagamento de Fatura (apenas com keywords explícitas)
    if ("pagamento de fatura" in t) or ("paguei a fatura" in t):
        cat = _category_before_comma(text)
        if not cat:
            m = re.search(r"cart[aã]o\s+([a-z0-9 ]+)", t)
            cat = f"Cartão {_titlecase(m.group(1))}" if m and m.group(1) else "Cartão"
        return GROUP_EMOJI["PAG_FATURA"], cat

    # Ganhos — prioriza Vendas
    if "vendas" in t: return GROUP_EMOJI["GANHOS"], "Vendas"
    if "salário" in t or "salario" in t: return GROUP_EMOJI["GANHOS"], "Salário"
    if re.search(r"\b(recebi|ganhei)\b", t): return GROUP_EMOJI["GANHOS"], "Ganhos"

    # Assinaturas (nomes comuns)
    assin = ["netflix", "amazon", "prime video", "disney", "disney+", "globoplay", "spotify", "hbo", "max", "apple tv", "youtube premium"]
    for a in assin:
        if a in t:
            return GROUP_EMOJI["ASSINATURA"], _titlecase(a.replace("+", "+"))

    # Fixos
    if "aluguel" in t:  return GROUP_EMOJI["GASTOS_FIXOS"], "Aluguel"
    if "água" in t or "agua" in t: return GROUP_EMOJI["GASTOS_FIXOS"], "Agua"
    if "energia" in t or "luz" in t: return GROUP_EMOJI["GASTOS_FIXOS"], "Energia"
    if "internet" in t: return GROUP_EMOJI["GASTOS_FIXOS"], "Internet"
    if "condomínio" in t or "condominio" in t: return GROUP_EMOJI["GASTOS_FIXOS"], "Condomínio"

    # Variáveis comuns
    if "ifood" in t: return GROUP_EMOJI["GASTOS_VARIAVEIS"], "ifood"
    if "mercado" in t: return GROUP_EMOJI["GASTOS_VARIAVEIS"], "mercado"
    if any(w in t for w in ["restaurante","lanche","pizza","hamburg","sushi","rappi","uber","99"]):
        return GROUP_EMOJI["GASTOS_VARIAVEIS"], _category_before_comma(text) or "Outros"

    # Fallback → Variáveis
    return GROUP_EMOJI["GASTOS_VARIAVEIS"], _category_before_comma(text) or "Outros"

def parse_natural(text: str) -> Tuple[Optional[List], Optional[str]]:
    """
    Saída para planilha:
      [data_br, tipo, group_label, category, desc, valor, forma, cond]
    """
    valor = parse_money(text)
    if valor is None:
        return None, "Não achei o valor. Ex.: 45,90"

    data_br = parse_date(text) or _local_today().strftime("%d/%m/%Y")
    forma = detect_payment(text)
    cond = detect_installments(text)

    group_label, category = detect_group_and_category_free(text)

    # Em Pagamento de Fatura, forma nunca é "💳cartão ..."
    if group_label == GROUP_EMOJI["PAG_FATURA"] and str(forma).startswith("💳cartão"):
        t_low = text.lower()
        if "pix" in t_low: forma = "Pix"
        elif ("débito" in t_low) or ("debito" in t_low): forma = "débito"
        else: forma = "Outros"

    # Tipo por grupo (fatura = sempre saída)
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
# ===========================
# Google Auth helpers
# ===========================
def _build_service_sheets():
    creds = None
    if GOOGLE_USE_OAUTH and os.path.exists(GOOGLE_TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(GOOGLE_TOKEN_PATH, GOOGLE_OAUTH_SCOPES)
    elif GOOGLE_SA_JSON:
        info = json.loads(GOOGLE_SA_JSON)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES_SA)
    if not creds:
        raise RuntimeError("Nenhuma credencial Google configurada.")
    return build("sheets", "v4", credentials=creds)

def _build_service_drive():
    creds = None
    if GOOGLE_USE_OAUTH and os.path.exists(GOOGLE_TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(GOOGLE_TOKEN_PATH, GOOGLE_OAUTH_SCOPES)
    elif GOOGLE_SA_JSON:
        info = json.loads(GOOGLE_SA_JSON)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES_SA)
    if not creds:
        raise RuntimeError("Nenhuma credencial Google configurada.")
    return build("drive", "v3", credentials=creds)

# ===========================
# Sheets / Drive actions
# ===========================
def sheet_append_row(sheet_id: str, tab: str, row: list):
    service = _build_service_sheets()
    range_ = f"{tab}!{SHEET_FIRST_COL}{SHEET_START_ROW}:{SHEET_LAST_COL}"
    body = {"values": [row]}
    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=range_,
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()

def sheet_update_license_email(license_key: str, email: str):
    if not LICENSE_SHEET_ID:
        return
    service = _build_service_sheets()
    values = service.spreadsheets().values().get(
        spreadsheetId=LICENSE_SHEET_ID,
        range=f"{LICENSE_SHEET_TAB}!A:F"
    ).execute().get("values", [])
    for i, row in enumerate(values, start=1):
        if row and row[0] == license_key:
            rng = f"{LICENSE_SHEET_TAB}!E{i}"
            service.spreadsheets().values().update(
                spreadsheetId=LICENSE_SHEET_ID,
                range=rng,
                valueInputOption="RAW",
                body={"values": [[email]]}
            ).execute()
            return

async def setup_client_file(chat_id: str, email: str):
    try:
        service_drive = _build_service_drive()
        # copia arquivo modelo
        copy = service_drive.files().copy(
            fileId=GS_TEMPLATE_ID,
            body={
                "parents": [GS_DEST_FOLDER_ID],
                "name": f"Planilha - {email}",
            }
        ).execute()
        fid = copy["id"]
        # compartilhar
        service_drive.permissions().create(
            fileId=fid,
            body={"type": "user", "role": SHARE_LINK_ROLE, "emailAddress": email},
            sendNotificationEmail=True
        ).execute()
        link = f"https://docs.google.com/spreadsheets/d/{fid}"
        return True, None, link
    except Exception as e:
        return False, str(e), None

# ===========================
# Telegram Webhook
# ===========================
@app.post("/telegram/webhook")
async def telegram_webhook(request: Request, x_telegram_bot_api_secret_token: Optional[str] = Header(None)):
    if TELEGRAM_WEBHOOK_SECRET and x_telegram_bot_api_secret_token != TELEGRAM_WEBHOOK_SECRET:
        return {"ok": False, "error": "unauthorized"}

    data = await request.json()
    message = data.get("message") or data.get("callback_query", {}).get("message")
    if not message:
        return {"ok": True}

    chat_id = str(message["chat"]["id"])
    text = (
        data.get("message", {}).get("text")
        or data.get("callback_query", {}).get("data")
        or ""
    ).strip()

    # Callback de grupo
    if text.startswith("grp:"):
        group_key = text.split(":", 1)[1]
        set_selected_group(chat_id, group_key)
        example = GROUP_EXAMPLE.get(group_key, "Mercado, 59,90 no débito hoje")
        await tg_send(chat_id, f"✔ Grupo selecionado: {_group_label_by_key(group_key)}.\nAgora me envie o lançamento (ex.: *{example}*)")
        return {"ok": True}

    # /start
    if text.lower() == "/start":
        record_usage(chat_id, "start")
        set_selected_group(chat_id, None)
        set_pending(chat_id, "await_license", None)
        await tg_send(chat_id, "Olá! 👋\nPor favor, *informe sua licença* para começar (ex.: `GF-ABCD-1234`).")
        return {"ok": True}

    # /novo → mostrar botões
    if text.lower() == "/novo":
        kb = _group_keyboard_rows()
        await tg_send_with_kb(chat_id, "Escolha o grupo do lançamento:", kb)
        return {"ok": True}

    # /cancel
    if text.lower() == "/cancel":
        set_selected_group(chat_id, None)
        set_pending(chat_id, None, None)
        await tg_send(chat_id, "Operação cancelada. Envie /start para recomeçar.")
        return {"ok": True}

    # Etapas pendentes
    step, temp_license = get_pending(chat_id)
    if step == "await_license":
        lic = text.strip()
        con = _db(); cur = con.execute("SELECT license_key,status FROM licenses WHERE license_key=?", (lic,))
        row = cur.fetchone(); con.close()
        if not row:
            await tg_send(chat_id, "❌ Licença inválida. Tente novamente ou digite /cancel.")
            return {"ok": True}
        status = row[1]
        if status != "active":
            await tg_send(chat_id, f"❌ Esta licença está {status}.")
            return {"ok": True}
        con = _db()
        con.execute("INSERT OR REPLACE INTO clients(chat_id,license_key,created_at,last_seen_at) VALUES(?,?,?,?)",
                    (chat_id, lic, _now_iso(), _now_iso()))
        con.commit(); con.close()
        set_pending(chat_id, "await_email", lic)
        await tg_send(chat_id, "Licença ok ✅\nAgora me diga seu *e-mail* (ex.: `cliente@gmail.com`).")
        return {"ok": True}

    if step == "await_email":
        email = text.strip()
        if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
            await tg_send(chat_id, "❗ E-mail inválido. Tente novamente.")
            return {"ok": True}
        con = _db()
        con.execute("UPDATE clients SET email=?, last_seen_at=? WHERE chat_id=?", (email, _now_iso(), chat_id))
        con.commit(); con.close()
        try:
            sheet_update_license_email(temp_license, email)
        except Exception as e:
            logger.error(f"Erro ao atualizar email no Sheets: {e}")
        set_pending(chat_id, None, None)
        ok, err, link = await setup_client_file(chat_id, email)
        if not ok:
            await tg_send(chat_id, f"❌ Erro ao criar planilha: {err}")
            return {"ok": True}
        await tg_send(chat_id, f"🚀 Planilha criada com sucesso!\n🔗 {link}")
        await tg_send(chat_id, "Você pode digitar seus lançamentos (ex.: `Mercado, 59 no débito hoje`)\nOu usar */novo* para escolher o grupo.")
        return {"ok": True}

    # Pega grupo selecionado
    grp = get_selected_group(chat_id)

    # Parseia lançamento
    parsed, err = parse_natural(text)
    if err:
        await tg_send(chat_id, f"❌ {err}")
        return {"ok": True}

    data_br, tipo, grupo, categoria, desc, valor, forma, cond = parsed
    # se o usuário escolheu grupo, sobrescreve
    if grp:
        grupo = _group_label_by_key(grp)

    try:
        if LICENSE_SHEET_ID:
            sheet_append_row(LICENSE_SHEET_ID, WORKSHEET_NAME,
                             [data_br, tipo, grupo, categoria, desc, valor, forma, cond])
        await tg_send(chat_id, f"✅ Lançamento registrado:\n• {grupo}\n• {categoria}\n• R${valor:,.2f}\n• {forma} - {cond}\n• {data_br}")
        await tg_send_with_kb(chat_id, "📌 Novo lançamento?", _group_keyboard_rows())
    except Exception as e:
        logger.error(f"Erro ao lançar: {e}")
        await tg_send(chat_id, f"❌ Erro ao registrar lançamento: {e}")
    return {"ok": True}

@app.get("/")
async def root():
    return HTMLResponse("<h3>Bot de Lançamentos ativo ✅</h3>")
