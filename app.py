# app.py — Bot de Lançamentos com:
# - licenciamento (chat_id + e-mail)
# - cópia no SharePoint/OneDrive
# - envio de e-mail com link
# - ONBOARDING guiado (/start -> pede licença -> pede e-mail)
# - mantém compatibilidade com /start GF-XXX email@...

import os
import re
import json
import sqlite3
import secrets
import string
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List

import requests
import httpx
import msal
from fastapi import FastAPI, Request

# =========================================================
# FastAPI
# =========================================================
app = FastAPI()

# =========================================================
# ENVs (Telegram + Graph app-only)
# =========================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")  # opcional

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# === Excel destino ===
EXCEL_PATH = os.getenv("EXCEL_PATH")  # opcional (global); por chat usamos drive/item
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Plan1")
TABLE_NAME = os.getenv("TABLE_NAME", "Lancamentos")

# Estrutura por Drive/Item para cópia
DRIVE_ID = os.getenv("DRIVE_ID")
TEMPLATE_ITEM_ID = os.getenv("TEMPLATE_ITEM_ID")
DEST_FOLDER_ITEM_ID = os.getenv("DEST_FOLDER_ITEM_ID")

# =========================================================
# [LICENÇAS] ENVs / DB
# =========================================================
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")  # ID numérico do seu Telegram
SQLITE_PATH = os.getenv("SQLITE_PATH", "/tmp/db.sqlite")
LICENSE_ENFORCE = os.getenv("LICENSE_ENFORCE", "1") == "1"

def _db():
    return sqlite3.connect(SQLITE_PATH)

def licenses_db_init():
    """Cria/Ajusta tabelas necessárias (idempotente)."""
    con = _db()
    try:
        con.execute("""
        CREATE TABLE IF NOT EXISTS licenses (
            license_key TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'active',
            max_files INTEGER NOT NULL DEFAULT 1,
            expires_at TEXT,
            notes TEXT
        )""")
        con.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            chat_id TEXT PRIMARY KEY,
            license_key TEXT,
            file_scope TEXT,
            drive_id TEXT,
            item_id TEXT,
            created_at TEXT,
            last_seen_at TEXT,
            email TEXT,
            FOREIGN KEY (license_key) REFERENCES licenses(license_key)
        )""")
        # Sessões de onboarding (estado da conversa)
        con.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            chat_id TEXT PRIMARY KEY,
            stage TEXT,           -- awaiting_license | awaiting_email | None
            tmp_license TEXT,
            created_at TEXT,
            updated_at TEXT
        )""")
        # Migrações leves
        try: con.execute("ALTER TABLE clients ADD COLUMN email TEXT")
        except Exception: pass
        con.execute("""
        CREATE TABLE IF NOT EXISTS usage (
            chat_id TEXT,
            event TEXT,
            ts TEXT
        )""")
        con.commit()
    finally:
        con.close()

@app.on_event("startup")
def _on_startup():
    licenses_db_init()

def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def record_usage(chat_id, event):
    con = _db()
    try:
        con.execute("INSERT INTO usage(chat_id, event, ts) VALUES(?,?,?)",
                    (str(chat_id), event, _now_iso()))
        con.commit()
    finally:
        con.close()

def _gen_key(prefix="GF"):
    alphabet = string.ascii_uppercase + string.digits
    part = lambda n: "".join(secrets.choice(alphabet) for _ in range(n))
    return f"{prefix}-{part(4)}-{part(4)}"

def create_license(days: int|None = 30, max_files: int = 1, notes: str|None=None):
    key = _gen_key()
    expires_at = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat(timespec="seconds") if days else None
    con = _db()
    try:
        con.execute("INSERT INTO licenses(license_key,status,max_files,expires_at,notes) VALUES(?,?,?,?,?)",
                    (key, "active", max_files, expires_at, notes))
        con.commit()
    finally:
        con.close()
    return key, expires_at

def get_license(license_key: str):
    con = _db()
    try:
        cur = con.execute("SELECT license_key,status,max_files,expires_at,notes FROM licenses WHERE license_key=?",
                          (license_key,))
        row = cur.fetchone()
    finally:
        con.close()
    if not row: return None
    return {"license_key": row[0], "status": row[1], "max_files": row[2], "expires_at": row[3], "notes": row[4]}

def is_license_valid(lic: dict):
    if not lic: return False, "Licença não encontrada."
    if lic["status"] != "active": return False, "Licença não está ativa."
    if lic["expires_at"]:
        try:
            if datetime.now(timezone.utc) > datetime.fromisoformat(lic["expires_at"]):
                return False, "Licença expirada."
        except Exception:
            return False, "Validade da licença inválida."
    return True, None

def bind_license_to_chat(chat_id: str, license_key: str):
    con = _db()
    try:
        cur = con.execute("SELECT chat_id FROM clients WHERE license_key=? AND chat_id<>? LIMIT 1",
                          (license_key, str(chat_id)))
        if cur.fetchone():
            return False, "Essa licença já foi usada por outro Telegram."
        con.execute("INSERT OR IGNORE INTO clients(chat_id, created_at) VALUES(?,?)",
                    (str(chat_id), _now_iso()))
        con.execute("UPDATE clients SET license_key=?, last_seen_at=? WHERE chat_id=?",
                    (license_key, _now_iso(), str(chat_id)))
        con.commit()
        return True, None
    finally:
        con.close()

def get_client(chat_id: str):
    con = _db()
    try:
        cur = con.execute("""SELECT chat_id, license_key, file_scope, drive_id, item_id, created_at, last_seen_at, email
                             FROM clients WHERE chat_id=?""", (str(chat_id),))
        row = cur.fetchone()
    finally:
        con.close()
    if not row: return None
    return {"chat_id": row[0], "license_key": row[1], "file_scope": row[2], "drive_id": row[3],
            "item_id": row[4], "created_at": row[5], "last_seen_at": row[6], "email": row[7]}

def set_client_file(chat_id: str, file_scope: str, drive_id: Optional[str], item_id: str):
    con = _db()
    try:
        con.execute("""UPDATE clients SET file_scope=?, drive_id=?, item_id=?, last_seen_at=? WHERE chat_id=?""",
                    (file_scope, drive_id, item_id, _now_iso(), str(chat_id)))
        con.commit()
    finally:
        con.close()

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

def set_client_email(chat_id: str, email: str):
    if not EMAIL_RE.match(email or ""):
        return False, "E-mail inválido."
    con = _db()
    try:
        con.execute("UPDATE clients SET email=?, last_seen_at=? WHERE chat_id=?",
                    (email, _now_iso(), str(chat_id)))
        con.commit()
        return True, None
    finally:
        con.close()

def require_active_license(chat_id: str):
    cli = get_client(chat_id)
    if not cli:
        return False, "Para usar o bot você precisa **ativar sua licença**. Envie /start."
    lic = get_license(cli["license_key"]) if cli["license_key"] else None
    ok, err = is_license_valid(lic)
    if not ok:
        return False, f"Licença inválida: {err}\nFale com o suporte para renovar/ativar."
    return True, None

def require_email(chat_id: str):
    cli = get_client(chat_id)
    if not cli or not cli.get("email"):
        return False, "Precisamos do seu e-mail. Envie: /email seu@dominio.com"
    return True, None

# ===== sessions (estado do onboarding) =====
def get_session(chat_id: str):
    con = _db()
    try:
        cur = con.execute("SELECT stage, tmp_license FROM sessions WHERE chat_id=?", (str(chat_id),))
        row = cur.fetchone()
    finally:
        con.close()
    if not row: return None
    return {"stage": row[0], "tmp_license": row[1]}

def set_session(chat_id: str, stage: Optional[str], tmp_license: Optional[str] = None):
    con = _db()
    try:
        con.execute("INSERT OR IGNORE INTO sessions(chat_id, stage, tmp_license, created_at, updated_at) VALUES(?,?,?,?,?)",
                    (str(chat_id), stage, tmp_license, _now_iso(), _now_iso()))
        con.execute("UPDATE sessions SET stage=?, tmp_license=?, updated_at=? WHERE chat_id=?",
                    (stage, tmp_license, _now_iso(), str(chat_id)))
        con.commit()
    finally:
        con.close()

def clear_session(chat_id: str):
    con = _db()
    try:
        con.execute("DELETE FROM sessions WHERE chat_id=?", (str(chat_id),))
        con.commit()
    finally:
        con.close()

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
# MSAL (app-only) + Graph helpers
# =========================================================
SCOPE = ["https://graph.microsoft.com/.default"]

def msal_token():
    app_msal = msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        client_credential=CLIENT_SECRET,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    )
    result = app_msal.acquire_token_silent(SCOPE, account=None)
    if not result:
        result = app_msal.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"MSAL error: {result}")
    return result["access_token"]

# --- copy helpers / Excel helpers ---
async def _graph_copy_file(template_item_id: str, drive_id: str, dest_folder_id: str, new_file_name: str) -> Optional[str]:
    token = msal_token()
    source_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{template_item_id}/copy"
    payload = {"parentReference": {"driveId": drive_id, "id": dest_folder_id}, "name": new_file_name}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(source_url, headers={"Authorization": f"Bearer {token}"}, json=payload)
    if r.status_code == 409:
        raise RuntimeError(f"Erro ao iniciar cópia do template (409 nameAlreadyExists) para '{new_file_name}'")
    if r.status_code != 202:
        raise RuntimeError(f"Erro ao iniciar cópia do template. Código: {r.status_code}, Detalhe: {r.text}")

    location = r.headers.get("Location")
    if location:
        async with httpx.AsyncClient(timeout=12) as client:
            for _ in range(10):
                await asyncio.sleep(1.2)
                pr = await client.get(location, headers={"Authorization": f"Bearer {token}"})
                if pr.status_code == 200:
                    data = pr.json()
                    return data.get("id") or data.get("resourceId") or data.get("itemId")

    # fallback: listar filhos e achar pelo nome
    await asyncio.sleep(3)
    safe_name = new_file_name.replace("'", "''")
    search_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{dest_folder_id}/children?$filter=name eq '{safe_name}'"
    async with httpx.AsyncClient(timeout=20) as client:
        search_r = await client.get(search_url, headers={"Authorization": f"Bearer {token}"})
    if search_r.status_code >= 300:
        raise RuntimeError(f"Falha ao buscar o arquivo copiado. Detalhe: {search_r.text}")
    data = search_r.json()
    if data.get('value'):
        return data['value'][0].get('id')
    raise RuntimeError("Não foi possível encontrar o ID da planilha recém-criada após a cópia.")

def _build_workbook_rows_add_url(excel_path: str) -> str:
    if "/drive/items/" in excel_path:
        return f"{GRAPH_BASE}{excel_path}/workbook/worksheets('{WORKSHEET_NAME}')/tables('{TABLE_NAME}')/rows/add"
    else:
        return f"{GRAPH_BASE}{excel_path}:/workbook/worksheets('{WORKSHEET_NAME}')/tables('{TABLE_NAME}')/rows/add"

def excel_path_for_chat(chat_id: str) -> str:
    cli = get_client(chat_id)
    if cli and cli.get("item_id") and cli.get("drive_id"):
        return f"/drives/{cli['drive_id']}/items/{cli['item_id']}"
    if EXCEL_PATH:
        return EXCEL_PATH
    raise RuntimeError(f"Caminho do Excel não configurado para o chat_id {chat_id}.")

def excel_add_row(values: List, chat_id: str):
    if len(values) != 8:
        raise RuntimeError(f"Esperava 8 colunas, recebi {len(values)}.")
    excel_path = excel_path_for_chat(chat_id)
    token = msal_token()
    url = _build_workbook_rows_add_url(excel_path)
    r = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json={"values": [values]}, timeout=25)
    if r.status_code >= 300:
        raise RuntimeError(f"Graph error {r.status_code}: {r.text}")
    return r.json()

# ====== EMAIL / SHARE-LINK ======
EMAIL_SEND_ENABLED = os.getenv("EMAIL_SEND_ENABLED", "0") == "1"
MAIL_SENDER_UPN = os.getenv("MAIL_SENDER_UPN")
MAIL_SENDER_USER_ID = os.getenv("MAIL_SENDER_USER_ID")
SHARE_LINK_TYPE = os.getenv("SHARE_LINK_TYPE", "view")
SHARE_LINK_SCOPE = os.getenv("SHARE_LINK_SCOPE", "anonymous")

def graph_create_share_link(drive_id: str, item_id: str, link_type: str = None, scope: str = None) -> str:
    token = msal_token()
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/createLink"
    payload = {"type": (link_type or SHARE_LINK_TYPE), "scope": (scope or SHARE_LINK_SCOPE)}
    r = requests.post(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                      json=payload, timeout=20)
    if r.status_code >= 300:
        raise RuntimeError(f"createLink error {r.status_code}: {r.text}")
    data = r.json()
    return (data.get("link") or {}).get("webUrl") or (data.get("link") or {}).get("url")

def graph_send_mail(to_email: str, subject: str, html_body: str):
    if not (MAIL_SENDER_UPN or MAIL_SENDER_USER_ID):
        raise RuntimeError("Configure MAIL_SENDER_UPN ou MAIL_SENDER_USER_ID para enviar e-mail.")
    token = msal_token()
    sender_path = MAIL_SENDER_USER_ID or MAIL_SENDER_UPN
    send_url = f"{GRAPH_BASE}/users/{sender_path}/sendMail"
    msg = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": to_email}}],
        },
        "saveToSentItems": True
    }
    r = requests.post(send_url,
                      headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                      json=msg, timeout=20)
    if r.status_code not in (202, 200):
        raise RuntimeError(f"sendMail error {r.status_code}: {r.text}")

# ===== criação/reatribuição da planilha, nomeando com o e-mail =====
async def setup_client_file(chat_id: str) -> Tuple[bool, Optional[str]]:
    """Cria ou reaproveita a planilha do cliente, nomeando-a com o e-mail."""
    cli = get_client(chat_id)
    if cli and cli["item_id"]:
        return True, None

    # garantir que temos o e-mail salvo
    to_email = (cli or {}).get("email") if cli else None
    if not to_email:
        # fallback: usa chat_id para não quebrar (mas o onboarding sempre preenche e-mail antes)
        to_email = str(chat_id)
    safe_email = re.sub(r'[^A-Za-z0-9@._-]', '_', to_email)
    base_name = f"Lancamentos - {safe_email}.xlsx"
    token = msal_token()

    # 1) tentar reaproveitar arquivo já existente com esse nome
    try:
        safe_name = base_name.replace("'", "''")
        search_url = f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{DEST_FOLDER_ITEM_ID}/children?$filter=name eq '{safe_name}'"
        r = requests.get(search_url, headers={"Authorization": f"Bearer {token}"}, timeout=15)
        if r.status_code < 300:
            data = r.json()
            if data.get("value"):
                item_id = data["value"][0]["id"]
                set_client_file(str(chat_id), "drive", DRIVE_ID, item_id)
                return True, None
    except Exception:
        pass

    # 2) criar cópia
    try:
        new_item_id = await _graph_copy_file(TEMPLATE_ITEM_ID, DRIVE_ID, DEST_FOLDER_ITEM_ID, base_name)
        if not new_item_id:
            return False, "Não foi possível obter o ID da nova planilha."
        set_client_file(str(chat_id), "drive", DRIVE_ID, new_item_id)
        return True, None
    except Exception as e:
        if "nameAlreadyExists" in str(e):
            try:
                suffix = datetime.now().strftime("%Y%m%d-%H%M%S")
                alt_name = f"Lancamentos - {safe_email} - {suffix}.xlsx"
                new_item_id = await _graph_copy_file(TEMPLATE_ITEM_ID, DRIVE_ID, DEST_FOLDER_ITEM_ID, alt_name)
                if not new_item_id:
                    return False, "Não foi possível obter o ID da planilha (fallback)."
                set_client_file(str(chat_id), "drive", DRIVE_ID, new_item_id)
                return True, None
            except Exception as e2:
                return False, f"Falha ao criar planilha (fallback): {e2}"
        return False, f"Falha ao criar planilha: {e}"

# =========================================================
# NLP / Parsers
# =========================================================
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

def detect_payment(text: str) -> str:
    t = text.lower()
    m = re.search(r"cart[aã]o\s+([a-z0-9 ]+)", t)
    if m:
        brand = m.group(1).strip()
        brand = re.sub(r"\s+", " ", brand).strip()
        return f"💳 cartão {brand}"
    if "pix" in t: return "Pix"
    if "dinheiro" in t or "cash" in t: return "Dinheiro"
    if "débito" in t or "debito" in t: return "Débito"
    if "crédito" in t or "credito" in t: return "💳 cartão"
    return "Outros"

def detect_installments(text: str) -> str:
    t = text.lower()
    m = re.search(r"(\d{1,2})x", t)
    if m: return f"{m.group(1)}x"
    if "parcelad" in t: return "parcelado"
    if "à vista" in t or "a vista" in t or "avista" in t: return "à vista"
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
    "Aluguel": ["aluguel", "condomínio"], "Água": ["água", "sabesp"], "Energia": ["energia", "luz"],
    "Internet": ["internet", "banda larga", "fibra"], "Plano de Saúde": ["plano de saúde", "unimed", "amil"],
    "Escola": ["escola", "mensalidade", "faculdade", "curso"], "Imposto": ["iptu", "ipva"],
    "Financiamento": ["financiamento", "parcela do carro", "parcela da casa"],
}

def map_group(category: str) -> str:
    if category in ["Aluguel","Água","Energia","Internet","Plano de Saúde","Escola","Assinatura"]: return "Gastos Fixos"
    if category in ["Imposto","Financiamento","Empréstimo"]: return "Despesas Temporárias"
    if category in ["Mercado","Farmácia","Combustível","Passeio em família","Ifood","Viagem","Restaurante"]: return "Gastos Variáveis"
    if category in ["Salário","Vale","Renda Extra 1","Renda Extra 2","Pró labore"]: return "Ganhos"
    if category in ["Renda Fixa","Renda Variável","Fundos imobiliários"]: return "Investimento"
    if category in ["Trocar de carro","Viagem pra Disney"]: return "Reserva"
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
                    if raw and len(raw) < 60: desc = raw
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
# Routes
# =========================================================
@app.get("/")
def root():
    return {"status": "ok"}

@app.post("/telegram/webhook")
async def telegram_webhook(req: Request):
    if TELEGRAM_WEBHOOK_SECRET:
        header = req.headers.get("x-telegram-bot-api-secret-token")
        if header != TELEGRAM_WEBHOOK_SECRET:
            return {"ok": False, "status": "forbidden"}

    body = await req.json()
    message = body.get("message") or {}
    chat_id = message.get("chat", {}).get("id")
    text = (message.get("text") or "").strip()

    if not chat_id or not text:
        return {"ok": True}

    chat_id_str = str(chat_id)
    low = text.lower()

    # ===== comandos utilitários =====
    if low == "/cancel":
        clear_session(chat_id_str)
        await tg_send(chat_id, "Onboarding cancelado. Envie /start quando quiser recomeçar.")
        return {"ok": True}

    # ===== [ADMIN] comandos de licença =====
    if ADMIN_TELEGRAM_ID and chat_id_str == ADMIN_TELEGRAM_ID:
        if low.startswith("/licenca nova"):
            try:
                parts = text.split()
                days = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 30
            except:
                days = 30
            key, exp = create_license(days=None if days == 0 else days)
            msg = f"🔑 *Licença criada:*\n`{key}`\n*Validade:* {'vitalícia' if not exp else exp}"
            await tg_send(chat_id, msg)
            return {"ok": True}
        if low.startswith("/licenca info"):
            await tg_send(chat_id, f"Seu ADMIN ID ({chat_id_str}) está correto. Bot ativo.")
            return {"ok": True}
        if low.startswith("/licenca"):
            await tg_send(chat_id, "Comando de licença não reconhecido ou incompleto.")
            return {"ok": True}

    # ===== Onboarding guiado =====
    # 0) Compatibilidade com /start <lic> <email>
    if low.startswith("/start"):
        record_usage(chat_id, "start")
        parts = text.split()
        token = parts[1].strip() if len(parts) > 1 else None
        email = parts[2].strip() if len(parts) > 2 else None

        # Se veio completo, segue fluxo clássico
        if token:
            lic = get_license(token)
            ok, err = is_license_valid(lic)
            if not ok:
                await tg_send(chat_id, f"❌ Licença inválida: {err}")
                return {"ok": True}
            ok2, err2 = bind_license_to_chat(chat_id_str, token)
            if not ok2:
                await tg_send(chat_id, f"❌ {err2}")
                return {"ok": True}
            if email:
                ok3, err3 = set_client_email(chat_id_str, email)
                if not ok3:
                    await tg_send(chat_id, f"❌ {err3}")
                    return {"ok": True}
            else:
                await tg_send(chat_id, "Licença ok ✅\nAgora me diga seu e-mail (ex.: `cliente@gmail.com`).")
                set_session(chat_id_str, "awaiting_email")
                return {"ok": True}

            await tg_send(chat_id, "✅ Licença ativada. Configurando sua planilha de lançamentos...")
            file_ok, file_err = await setup_client_file(chat_id_str)
            if not file_ok:
                await tg_send(chat_id, f"❌ Erro na configuração da planilha. Fale com o suporte: {file_err}")
                return {"ok": True}
            await tg_send(chat_id, "🚀 Planilha configurada com sucesso!")
            # e-mail com link (se habilitado)
            try:
                if EMAIL_SEND_ENABLED:
                    cli = get_client(chat_id_str)
                    to_email = cli.get("email") if cli else None
                    drive_id = cli.get("drive_id") if cli else None
                    item_id  = cli.get("item_id") if cli else None
                    if to_email and drive_id and item_id:
                        share_url = graph_create_share_link(drive_id, item_id, SHARE_LINK_TYPE, SHARE_LINK_SCOPE)
                        subj = "Sua planilha de lançamentos está pronta"
                        html = f"""<p>Olá!</p><p>Sua planilha foi criada com sucesso. Acesse: <a href="{share_url}">{share_url}</a></p>"""
                        graph_send_mail(to_email, subj, html)
                        await tg_send(chat_id, f"✉️ E-mail enviado para {to_email}")
            except Exception as e:
                await tg_send(chat_id, f"⚠️ Planilha criada, mas falhou enviar e-mail: {e}")

            reply = ("Pode me contar seus gastos/recebimentos.\n"
                     "Ex.: _gastei 45,90 no mercado via cartão hoje_")
            await tg_send(chat_id, reply)
            clear_session(chat_id_str)
            return {"ok": True}

        # Se veio só /start → inicia onboarding
        set_session(chat_id_str, "awaiting_license")
        await tg_send(chat_id, "Olá! 👋\nPor favor, **informe sua licença** (ex.: `GF-ABCD-1234`).\n\nVocê pode digitar /cancel para cancelar.")
        return {"ok": True}

    # 1) Estamos esperando LICENÇA?
    sess = get_session(chat_id_str)
    if sess and sess.get("stage") == "awaiting_license":
        # considera qualquer texto não-comando como tentativa de licença
        if text.startswith("/"):
            await tg_send(chat_id, "Por favor, envie apenas a *licença* (ex.: `GF-ABCD-1234`) ou /cancel.")
            return {"ok": True}
        token = text.strip()
        lic = get_license(token)
        ok, err = is_license_valid(lic)
        if not ok:
            await tg_send(chat_id, f"❌ Licença inválida: {err}\nTente novamente ou digite /cancel.")
            return {"ok": True}
        ok2, err2 = bind_license_to_chat(chat_id_str, token)
        if not ok2:
            await tg_send(chat_id, f"❌ {err2}\nTente outra licença ou /cancel.")
            return {"ok": True}
        set_session(chat_id_str, "awaiting_email")
        await tg_send(chat_id, "Licença ok ✅\nAgora me diga seu **e-mail** (ex.: `cliente@gmail.com`).")
        return {"ok": True}

    # 2) Estamos esperando E-MAIL?
    if sess and sess.get("stage") == "awaiting_email":
        if text.startswith("/"):
            await tg_send(chat_id, "Por favor, envie apenas o *e-mail* (ex.: `cliente@gmail.com`) ou /cancel.")
            return {"ok": True}
        email = text.strip()
        if not EMAIL_RE.match(email):
            await tg_send(chat_id, "❌ E-mail inválido. Tente algo como `cliente@gmail.com`.")
            return {"ok": True}
        ok3, err3 = set_client_email(chat_id_str, email)
        if not ok3:
            await tg_send(chat_id, f"❌ {err3}")
            return {"ok": True}

        await tg_send(chat_id, "✅ Obrigado! Configurando sua planilha de lançamentos...")
        file_ok, file_err = await setup_client_file(chat_id_str)
        if not file_ok:
            await tg_send(chat_id, f"❌ Erro na configuração da planilha. Fale com o suporte: {file_err}")
            return {"ok": True}
        await tg_send(chat_id, "🚀 Planilha configurada com sucesso!")

        # enviar e-mail com link (se habilitado)
        try:
            if EMAIL_SEND_ENABLED:
                cli = get_client(chat_id_str)
                to_email = cli.get("email") if cli else None
                drive_id = cli.get("drive_id") if cli else None
                item_id  = cli.get("item_id") if cli else None
                if to_email and drive_id and item_id:
                    share_url = graph_create_share_link(drive_id, item_id, SHARE_LINK_TYPE, SHARE_LINK_SCOPE)
                    subj = "Sua planilha de lançamentos está pronta"
                    html = f"""<p>Olá!</p><p>Sua planilha foi criada com sucesso. Acesse: <a href="{share_url}">{share_url}</a></p>"""
                    graph_send_mail(to_email, subj, html)
                    await tg_send(chat_id, f"✉️ E-mail enviado para {to_email}")
        except Exception as e:
            await tg_send(chat_id, f"⚠️ Planilha criada, mas falhou enviar e-mail: {e}")

        await tg_send(chat_id, "Tudo certo! Agora pode me contar seus gastos/recebimentos. Ex.: _gastei 45,90 no mercado via cartão hoje_")
        clear_session(chat_id_str)
        return {"ok": True}

    # ===== exige licença e e-mail antes de lançar (uso normal) =====
    if LICENSE_ENFORCE:
        ok, msg = require_active_license(chat_id_str)
        if not ok:
            await tg_send(chat_id, f"❗ {msg}")
            return {"ok": True}
        ok2, msg2 = require_email(chat_id_str)
        if not ok2:
            await tg_send(chat_id, f"❗ {msg2}")
            return {"ok": True}

    # ===== Processamento de Lançamentos =====
    row, err = parse_natural(text)
    if err:
        await tg_send(chat_id, f"❗ {err}")
        return {"ok": True}

    try:
        excel_add_row(row, chat_id_str)
        await tg_send(chat_id, "✅ Lançado!")
    except Exception as e:
        await tg_send(chat_id, f"❌ Erro ao lançar na planilha: {e}")

    return {"ok": True}
