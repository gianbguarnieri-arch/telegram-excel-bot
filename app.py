import os
import re
import json
import sqlite3
import secrets
import string
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict

import requests
import httpx
from fastapi import FastAPI, Request, Header

# Google APIs
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# =========================================================
# FastAPI
# =========================================================
app = FastAPI()


# =========================================================
# ENVs (Telegram + Google)
# =========================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")

SQLITE_PATH = os.getenv("SQLITE_PATH", "/tmp/db.sqlite")

# Google Service Account JSON (conteúdo completo)
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")

# IDs do modelo e pasta de destino
GS_TEMPLATE_ID = os.getenv("GS_TEMPLATE_ID")
GS_DEST_FOLDER_ID = os.getenv("GS_DEST_FOLDER_ID")

# Opções
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Plan1")
SHARE_LINK_ROLE = os.getenv("SHARE_LINK_ROLE", "writer")  # writer|commenter|reader
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()

# Scopes Google
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]


# =========================================================
# DB
# =========================================================
def _db():
    return sqlite3.connect(SQLITE_PATH)


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def licenses_db_init():
    """Inicializa/atualiza o schema."""
    con = _db()
    cur = con.cursor()
    # licenses
    cur.execute("""
    CREATE TABLE IF NOT EXISTS licenses (
        license_key TEXT PRIMARY KEY,
        status TEXT NOT NULL DEFAULT 'active',
        max_files INTEGER NOT NULL DEFAULT 1,
        expires_at TEXT,
        notes TEXT
    )""")
    # clients
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clients (
        chat_id TEXT PRIMARY KEY,
        license_key TEXT,
        email TEXT,
        file_scope TEXT,
        item_id TEXT,         -- spreadsheetId (Google)
        created_at TEXT,
        last_seen_at TEXT,
        FOREIGN KEY (license_key) REFERENCES licenses(license_key)
    )""")
    # usage log
    cur.execute("""
    CREATE TABLE IF NOT EXISTS usage (
        chat_id TEXT,
        event TEXT,
        ts TEXT
    )""")
    # Conversa pendente (/start → licença → e-mail)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pending (
        chat_id TEXT PRIMARY KEY,
        step TEXT,            -- 'await_license' | 'await_email'
        temp_license TEXT,
        created_at TEXT
    )""")

    # Migrations simples (adicionar colunas se faltarem)
    # email na clients (caso seja um banco antigo)
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
    con.commit()
    con.close()


def _gen_key(prefix="GF"):
    alphabet = string.ascii_uppercase + string.digits
    part = lambda n: "".join(secrets.choice(alphabet) for _ in range(n))
    return f"{prefix}-{part(4)}-{part(4)}"


def create_license(days: Optional[int] = 30, max_files: int = 1, notes: Optional[str] = None, custom_key: Optional[str] = None):
    key = custom_key or _gen_key()
    expires_at = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat(timespec="seconds") if days else None
    con = _db()
    con.execute("INSERT INTO licenses(license_key,status,max_files,expires_at,notes) VALUES(?,?,?,?,?)",
                (key, "active", max_files, expires_at, notes))
    con.commit()
    con.close()
    return key, expires_at


def get_license(license_key: str):
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

    con.execute("""
        INSERT OR IGNORE INTO clients(chat_id, created_at) VALUES(?,?)
    """, (str(chat_id), _now_iso()))

    con.execute("""
        UPDATE clients SET license_key=?, last_seen_at=? WHERE chat_id=?
    """, (license_key, _now_iso(), str(chat_id)))
    con.commit()
    con.close()
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
    con.commit()
    con.close()


def set_client_file(chat_id: str, item_id: str):
    con = _db()
    con.execute("""UPDATE clients SET file_scope=?, item_id=?, last_seen_at=? WHERE chat_id=?""",
                ("google", item_id, _now_iso(), str(chat_id)))
    con.commit()
    con.close()


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
    con.commit()
    con.close()


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


# =========================================================
# Telegram helper
# =========================================================
async def tg_send(chat_id, text):
    async with httpx.AsyncClient(timeout=12) as client:
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
        )


# =========================================================
# Google helpers
# =========================================================
def google_services():
    if not GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON não configurado.")
    info = json.loads(GOOGLE_SA_JSON)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    drive = build("drive", "v3", credentials=creds)
    sheets = build("sheets", "v4", credentials=creds)
    return drive, sheets


def drive_find_in_folder(service, folder_id: str, name: str) -> Optional[str]:
    """Retorna o ID do arquivo com 'name' dentro da pasta 'folder_id', ou None."""
    safe_name = name.replace("'", "\\'")
    q = f"name = '{safe_name}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(
        q=q,
        spaces="drive",
        fields="files(id,name)",
        pageSize=1
    ).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None


def drive_copy_template(new_name: str) -> str:
    """Copia o template para a pasta destino e retorna o spreadsheetId."""
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


def drive_create_anyone_link(file_id: str, role: str = "writer") -> str:
    """Cria link 'qualquer pessoa com o link' e retorna webViewLink."""
    drive, _ = google_services()
    # cria permissão
    drive.permissions().create(
        fileId=file_id,
        body={"type": "anyone", "role": role},
        fields="id"
    ).execute()
    # obtém o link
    meta = drive.files().get(fileId=file_id, fields="webViewLink").execute()
    return meta.get("webViewLink")


def sheets_append_row(spreadsheet_id: str, sheet_name: str, values: List):
    """Append de uma linha (8 colunas) no Google Sheets."""
    _, sheets = google_services()
    body = {"values": [values]}
    rng = f"{sheet_name}!A:H"
    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=rng,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()


# =========================================================
# NLP e Parsers
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
        return f"💳 cartão {brand}"
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


def detect_category_and_desc(text: str) -> Tuple[str, Optional[str]]:
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
    tipo = "Entrada" if re.search(r"\b(ganhei|recebi|sal[aá]rio|renda)\b", text.lower()) else "Saída"
    grupo = map_group(cat)

    return [data_iso, tipo, grupo, cat, (desc or ""), float(valor), forma, cond], None


# =========================================================
# Setup do arquivo por cliente (Google)
# =========================================================
async def setup_client_file(chat_id: str, email: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Cria (se necessário) a planilha do cliente e retorna (ok, erro, web_link).
    """
    cli = get_client(chat_id)
    if cli and cli.get("item_id"):
        try:
            link = drive_create_anyone_link(cli["item_id"], SHARE_LINK_ROLE)
        except Exception:
            link = None
        return True, None, link

    new_file_name = f"Lancamentos - {email}"
    try:
        new_id = drive_copy_template(new_file_name)
        # cria link público
        web_link = drive_create_anyone_link(new_id, SHARE_LINK_ROLE)
    except HttpError as e:
        return False, f"Falha Google API: {e}", None
    except Exception as e:
        return False, f"Falha ao criar planilha: {e}", None

    set_client_file(str(chat_id), new_id)
    return True, None, web_link


def add_row_to_client(values: List, chat_id: str):
    if len(values) != 8:
        raise RuntimeError(f"Esperava 8 colunas, recebi {len(values)}.")
    cli = get_client(chat_id)
    if not cli or not cli.get("item_id"):
        raise RuntimeError("Planilha do cliente não configurada.")
    sheets_append_row(cli["item_id"], WORKSHEET_NAME, values)


# =========================================================
# Routes
# =========================================================
@app.on_event("startup")
def _startup():
    # Inicializa/migra DB
    licenses_db_init()
    print(f"✅ DB pronto em {SQLITE_PATH}")


@app.get("/")
def root():
    return {"status": "ok"}


@app.post("/telegram/webhook")
async def telegram_webhook(
    req: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(default=None)
):
    # Valida secret (se configurado)
    if TELEGRAM_WEBHOOK_SECRET:
        if (x_telegram_bot_api_secret_token or "") != TELEGRAM_WEBHOOK_SECRET:
            return {"ok": True}  # ignora silenciosamente

    body = await req.json()
    message = body.get("message") or {}
    chat_id = message.get("chat", {}).get("id")
    text = (message.get("text") or "").strip()

    if not chat_id or not text:
        return {"ok": True}

    chat_id_str = str(chat_id)

    # ===== [ADMIN] comandos de licença =====
    if ADMIN_TELEGRAM_ID and chat_id_str == ADMIN_TELEGRAM_ID:
        low = text.lower()

        # /licenca nova <DAYS>   (gera código aleatório)
        if low.startswith("/licenca nova"):
            parts = text.split()
            # permite opcionalmente custom key: /licenca nova CHAVE days
            custom_key = None
            days = 30
            try:
                # tenta "/licenca nova KEY DAYS"
                if len(parts) >= 4 and parts[2] and parts[3].isdigit():
                    custom_key = parts[2].strip()
                    days = int(parts[3])
                elif len(parts) >= 3 and parts[2].isdigit():
                    days = int(parts[2])
            except Exception:
                pass

            key, exp = create_license(days=None if days == 0 else days, custom_key=custom_key)
            msg = (
                f"🔑 *Licença criada:*\n`{key}`\n"
                f"*Validade:* {'vitalícia' if not exp else exp}"
            )
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

    # ========= /start (modo amigável) =========
    if text.lower() == "/start":
        record_usage(chat_id, "start")
        set_pending(chat_id_str, "await_license", None)
        await tg_send(chat_id,
            "Olá! 👋\nPor favor, *informe sua licença* (ex.: `GF-ABCD-1234`).\n\n"
            "Você pode digitar /cancel para cancelar."
        )
        return {"ok": True}

    # ========= /start TOKEN [email] (modo antigo - fallback) =========
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

        # temos email: segue configuração
        set_client_email(chat_id_str, email)
        await tg_send(chat_id, "✅ Obrigado! Configurando sua planilha de lançamentos...")
        okf, errf, link = await setup_client_file(chat_id_str, email)
        if not okf:
            await tg_send(chat_id, f"❌ {errf}")
            return {"ok": True}

        await tg_send(chat_id, f"🚀 Planilha configurada com sucesso!\n🔗 {link}")
        await tg_send(chat_id,
            "Tudo certo! Agora pode me contar seus gastos/recebimentos. Ex.: _gastei 45,90 no mercado via cartão hoje_")
        return {"ok": True}

    # ========= Conversa pendente: aguardando licença ou e-mail =========
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
            await tg_send(chat_id, f"❌ {errf}")
            return {"ok": True}

        await tg_send(chat_id, f"🚀 Planilha configurada com sucesso!\n🔗 {link}")
        await tg_send(chat_id,
            "Tudo certo! Agora pode me contar seus gastos/recebimentos. Ex.: _gastei 45,90 no mercado via cartão hoje_")
        return {"ok": True}

    # ===== exige licença para qualquer uso (se habilitado por padrão) =====
    ok, msg = require_active_license(chat_id_str)
    if not ok:
        await tg_send(chat_id, f"❗ {msg}")
        return {"ok": True}

    # ===== Processamento de Lançamentos (NLP) =====
    row, err = parse_natural(text)
    if err:
        await tg_send(chat_id, f"❗ {err}")
        return {"ok": True}

    try:
        add_row_to_client(row, chat_id_str)
        await tg_send(chat_id, "✅ Lançado!")
    except Exception as e:
        await tg_send(chat_id, f"❌ Erro ao lançar na planilha: {e}")

    return {"ok": True}
