import os
import re
import json
import sqlite3
import secrets
import string
import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict

import requests
import httpx
import msal
from fastapi import FastAPI, Request, Header

# =========================================================
# FastAPI
# =========================================================
app = FastAPI()

# =========================================================
# ENVs (Telegram + Graph app-only)
# =========================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET")  # opcional

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# === Excel global (fallback) ===
EXCEL_PATH = os.getenv("EXCEL_PATH")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Plan1")
TABLE_NAME = os.getenv("TABLE_NAME", "Lancamentos")

# Estrutura por Drive/Item para cópia
DRIVE_ID = os.getenv("DRIVE_ID")
TEMPLATE_ITEM_ID = os.getenv("TEMPLATE_ITEM_ID")
DEST_FOLDER_ITEM_ID = os.getenv("DEST_FOLDER_ITEM_ID")

# === Compartilhamento / E-mail ===
SHARE_LINK_TYPE = os.getenv("SHARE_LINK_TYPE", "edit")           # "view" | "edit"
SHARE_LINK_SCOPE = os.getenv("SHARE_LINK_SCOPE", "anonymous")    # "anonymous" | "organization" | "users"
SHARE_LINK_PASSWORD = os.getenv("SHARE_LINK_PASSWORD") or None

EMAIL_SEND_ENABLED = os.getenv("EMAIL_SEND_ENABLED", "0") == "1"
MAIL_SENDER_UPN = os.getenv("MAIL_SENDER_UPN")  # ex.: Gian@SeuTenant.onmicrosoft.com

# =========================================================
# [LICENÇAS] ENVs / DB
# =========================================================
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")
# NOTA: O caminho deve ser /tmp/db.sqlite no Render Free
SQLITE_PATH = os.getenv("SQLITE_PATH", "/tmp/db.sqlite")
LICENSE_ENFORCE = os.getenv("LICENSE_ENFORCE", "1") == "1"

def _db():
    return sqlite3.connect(SQLITE_PATH)

def licenses_db_init():
    """Inicializa as tabelas do SQLite. Chamar APENAS uma vez (via console do Render)."""
    con = _db()
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
        FOREIGN KEY (license_key) REFERENCES licenses(license_key)
    )""")
    con.execute("""
    CREATE TABLE IF NOT EXISTS usage (
        chat_id TEXT,
        event TEXT,
        ts TEXT
    )""")
    con.commit(); con.close()

# A chamada automática foi removida de propósito.

def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def record_usage(chat_id, event):
    con = _db()
    con.execute("INSERT INTO usage(chat_id, event, ts) VALUES(?,?,?)",
                (str(chat_id), event, _now_iso()))
    con.commit(); con.close()

def _gen_key(prefix="GF"):
    alphabet = string.ascii_uppercase + string.digits
    part = lambda n: "".join(secrets.choice(alphabet) for _ in range(n))
    return f"{prefix}-{part(4)}-{part(4)}"

def create_license(days: int|None = 30, max_files: int = 1, notes: str|None=None):
    key = _gen_key()
    expires_at = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat(timespec="seconds") if days else None
    con = _db()
    con.execute("INSERT INTO licenses(license_key,status,max_files,expires_at,notes) VALUES(?,?,?,?,?)",
                (key, "active", max_files, expires_at, notes))
    con.commit(); con.close()
    return key, expires_at

def get_license(license_key: str):
    con = _db()
    cur = con.execute("SELECT license_key,status,max_files,expires_at,notes FROM licenses WHERE license_key=?",
                      (license_key,))
    row = cur.fetchone()
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
    cur = con.execute("""SELECT chat_id, license_key, file_scope, drive_id, item_id, created_at, last_seen_at
                         FROM clients WHERE chat_id=?""", (str(chat_id),))
    row = cur.fetchone()
    con.close()
    if not row: return None
    return {"chat_id": row[0], "license_key": row[1], "file_scope": row[2], "drive_id": row[3],
            "item_id": row[4], "created_at": row[5], "last_seen_at": row[6]}

def set_client_file(chat_id: str, file_scope: str, drive_id: Optional[str], item_id: str):
    con = _db()
    con.execute("""UPDATE clients SET file_scope=?, drive_id=?, item_id=?, last_seen_at=? WHERE chat_id=?""",
                (file_scope, drive_id, item_id, _now_iso(), str(chat_id)))
    con.commit(); con.close()

def require_active_license(chat_id: str):
    cli = get_client(chat_id)
    if not cli:
        return False, "Para usar o bot você precisa **ativar sua licença**. Envie /start."
    lic = get_license(cli["license_key"]) if cli["license_key"] else None
    ok, err = is_license_valid(lic)
    if not ok:
        return False, f"Licença inválida: {err}\nFale com o suporte para renovar/ativar."
    return True, None

# =========================================================
# Conversa: estado efêmero (licença / e-mail)
# =========================================================
# Em memória (suficiente para 1 processo do Render).
pending: Dict[str, Dict] = {}  # chat_id -> {step, license_key?, email?}

def reset_pending(chat_id: str):
    pending.pop(str(chat_id), None)

# =========================================================
# Telegram helper
# =========================================================
async def tg_send(chat_id, text):
    async with httpx.AsyncClient(timeout=15) as client:
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

# ---------- Itens / cópia / consistência ----------
def graph_get_item(drive_id: str, item_id: str) -> bool:
    token = msal_token()
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=12)
    return r.status_code == 200

def graph_wait_item_available(drive_id: str, item_id: str, timeout_sec: int = 25) -> bool:
    start = time.time()
    while time.time() - start < timeout_sec:
        if graph_get_item(drive_id, item_id):
            return True
        time.sleep(1.5)
    return False

async def _graph_copy_file(template_item_id: str, drive_id: str, dest_folder_id: str, new_file_name: str) -> Optional[str]:
    """
    Copia o arquivo modelo e retorna o ID do novo arquivo (item_id).
    - Polling até ~60s até o item aparecer.
    - Resolve 409 (nameAlreadyExists) gerando nome único.
    """
    if not all([template_item_id, drive_id, dest_folder_id]):
        raise ValueError("Variáveis de ambiente de template e destino devem estar configuradas.")

    token = msal_token()
    source_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{template_item_id}/copy"

    async def _try_copy(_name: str) -> Optional[str]:
        payload = {"parentReference": {"driveId": drive_id, "id": dest_folder_id}, "name": _name}
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(source_url, headers={"Authorization": f"Bearer {token}"}, json=payload)

        if r.status_code == 202:
            # polling: procurar pelo nome até aparecer na pasta
            search_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{dest_folder_id}/children"
            safe_name = _name.replace("'", "''")
            for _ in range(30):  # ~60s
                async with httpx.AsyncClient(timeout=15) as client:
                    sr = await client.get(search_url,
                        headers={"Authorization": f"Bearer {token}"},
                        params={"$filter": f"name eq '{safe_name}'"})
                if sr.status_code < 300:
                    data = sr.json()
                    if data.get("value"):
                        return data["value"][0]["id"]
                await asyncio.sleep(2)
            return None

        if r.status_code == 409:
            return "NAME_CONFLICT"

        raise RuntimeError(f"Erro ao iniciar cópia do template. Código: {r.status_code}, Detalhe: {r.text}")

    result = await _try_copy(new_file_name)
    if result == "NAME_CONFLICT":
        base = re.sub(r"\.xlsx$", "", new_file_name, flags=re.I)
        unique = datetime.now().strftime("-%Y%m%d-%H%M%S")
        new_name = f"{base}{unique}.xlsx"
        result = await _try_copy(new_name)

    if not result:
        raise RuntimeError("Não foi possível localizar a planilha copiada (timeout de propagação).")
    if result == "NAME_CONFLICT":
        raise RuntimeError("Conflito de nome persistente ao copiar a planilha.")
    return result

# ---------- Permissões / links ----------
def graph_list_permissions(drive_id: str, item_id: str) -> list:
    token = msal_token()
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/permissions"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=20)
    if r.status_code >= 300:
        raise RuntimeError(f"listPermissions error {r.status_code}: {r.text}")
    return r.json().get("value", [])

def graph_delete_permission(drive_id: str, item_id: str, perm_id: str):
    token = msal_token()
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/permissions/{perm_id}"
    r = requests.delete(url, headers={"Authorization": f"Bearer {token}"}, timeout=20)
    if r.status_code not in (204, 200):
        raise RuntimeError(f"deletePermission error {r.status_code}: {r.text}")

def graph_create_share_link(drive_id: str, item_id: str, link_type: str = None, scope: str = None, password: str | None = None) -> str:
    # Aguarda item estar visível
    graph_wait_item_available(drive_id, item_id, timeout_sec=25)

    # Revoga links antigos (evita reutilizar link "organization")
    try:
        perms = graph_list_permissions(drive_id, item_id)
        for p in perms:
            if p.get("link"):
                graph_delete_permission(drive_id, item_id, p["id"])
    except Exception:
        pass

    token = msal_token()
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/createLink"

    payload = {
        "type": (link_type or SHARE_LINK_TYPE),
        "scope": (scope or SHARE_LINK_SCOPE),
    }
    pwd = password if password is not None else SHARE_LINK_PASSWORD
    if (payload["scope"] == "anonymous") and pwd:
        payload["password"] = pwd

    last_err = None
    for attempt in range(6):
        r = requests.post(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                          json=payload, timeout=20)
        if r.status_code < 300:
            data = r.json()
            return (data.get("link") or {}).get("webUrl") or (data.get("link") or {}).get("url")
        if r.status_code == 404:
            last_err = r.text
            time.sleep(2 + attempt)
            continue
        raise RuntimeError(f"createLink error {r.status_code}: {r.text}")
    raise RuntimeError(f"createLink error 404 (after retries): {last_err}")

# ---------- Envio de e-mail ----------
def send_mail(subject: str, body_text: str, to_email: str):
    if not EMAIL_SEND_ENABLED:
        return
    if not MAIL_SENDER_UPN:
        raise RuntimeError("MAIL_SENDER_UPN não configurado.")
    token = msal_token()
    url = f"{GRAPH_BASE}/users/{MAIL_SENDER_UPN}/sendMail"
    msg = {
        "message": {
            "subject": subject,
            "body": {"contentType": "Text", "content": body_text},
            "toRecipients": [{"emailAddress": {"address": to_email}}],
        },
        "saveToSentItems": "false",
    }
    r = requests.post(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                      json=msg, timeout=20)
    if r.status_code >= 300:
        raise RuntimeError(f"sendMail error {r.status_code}: {r.text}")

# ---------- Excel (Workbook) ----------
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

# =========================================================
# Setup de arquivo do cliente
# =========================================================
async def setup_client_file(chat_id: str, email_for_name: Optional[str]) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Cria (se necessário) o arquivo do cliente e retorna (ok, err, share_url).
    """
    cli = get_client(chat_id)
    if cli and cli["item_id"]:
        try:
            share_url = graph_create_share_link(cli["drive_id"], cli["item_id"])
        except Exception as e:
            return True, f"Falhou ao gerar link de compartilhamento: {e}", None
        return True, None, share_url

    # Nome do arquivo
    slug = None
    if email_for_name:
        slug = re.sub(r"[^a-zA-Z0-9._@+-]", "_", email_for_name.strip())
    new_file_name = f"Lancamentos - {slug}.xlsx" if slug else f"Lancamentos - {chat_id}.xlsx"

    try:
        new_item_id = await _graph_copy_file(TEMPLATE_ITEM_ID, DRIVE_ID, DEST_FOLDER_ITEM_ID, new_file_name)
    except Exception as e:
        return False, f"Falha ao criar planilha: {e}", None

    if not new_item_id:
        return False, "Não foi possível obter o ID da nova planilha.", None

    set_client_file(str(chat_id), "drive", DRIVE_ID, new_item_id)

    # cria link
    try:
        share_url = graph_create_share_link(DRIVE_ID, new_item_id)
    except Exception as e:
        return True, f"Planilha criada, mas falhou enviar e-mail: {e}", None

    return True, None, share_url

# =========================================================
# NLP e Parsers (mantidos)
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
async def telegram_webhook(
    req: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(None)
):
    # Validar secret do webhook (se configurado)
    if TELEGRAM_WEBHOOK_SECRET and (x_telegram_bot_api_secret_token != TELEGRAM_WEBHOOK_SECRET):
        return {"ok": True}

    body = await req.json()
    message = body.get("message") or {}
    chat_id = message.get("chat", {}).get("id")
    text = (message.get("text") or "").strip()

    if not chat_id or not text:
        return {"ok": True}

    chat_id_str = str(chat_id)
    low = text.lower()

    # ===== Admin: criação de licenças =====
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
            await tg_send(chat_id, f"Seu ADMIN ID ({chat_id_str}) está correto. O bot está ativo.")
            return {"ok": True}

        if low.startswith("/licenca"):
            await tg_send(chat_id, "Comando de licença não reconhecido ou incompleto.")
            return {"ok": True}

    # ===== Cancelar fluxo =====
    if low.startswith("/cancel"):
        reset_pending(chat_id_str)
        await tg_send(chat_id, "✅ Cancelado. Envie /start para começar novamente.")
        return {"ok": True}

    # ===== Fluxo /start guiado =====
    if low.startswith("/start"):
        record_usage(chat_id, "start")

        # Se veio token na mesma linha (/start TOKEN ou /start TOKEN email)
        parts = text.split()
        if len(parts) >= 2 and re.match(r"^[A-Z]{2}-[A-Z0-9]{4}-[A-Z0-9]{4}$", parts[1].strip(), re.I):
            token = parts[1].upper()
            lic = get_license(token)
            ok, err = is_license_valid(lic)
            if not ok:
                await tg_send(chat_id, f"❌ Licença inválida: {err}")
                return {"ok": True}

            ok2, err2 = bind_license_to_chat(chat_id_str, token)
            if not ok2:
                await tg_send(chat_id, f"❌ {err2}")
                return {"ok": True}

            # Se tiver e-mail na mesma linha, usa; senão pergunta
            email_inline = None
            if len(parts) >= 3:
                email_inline = parts[2].strip()

            if email_inline and re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email_inline):
                await tg_send(chat_id, "✅ Licença ativada. Configurando sua planilha de lançamentos...")
                okf, errf, share = await setup_client_file(chat_id_str, email_inline)
                if not okf:
                    await tg_send(chat_id, f"❌ {errf}")
                    return {"ok": True}

                if EMAIL_SEND_ENABLED and share:
                    try:
                        send_mail(
                            subject="Sua planilha de lançamentos está pronta",
                            body_text=f"Olá!\n\nSua planilha foi criada com sucesso. Acesse: {share}\n",
                            to_email=email_inline
                        )
                        await tg_send(chat_id, f"✉️ E-mail enviado para `{email_inline}`")
                    except Exception as e:
                        await tg_send(chat_id, f"⚠️ Planilha criada, mas falhou enviar e-mail: {e}")

                await tg_send(chat_id, "🚀 Planilha configurada com sucesso!\n\nTudo certo! Agora pode me contar seus gastos/recebimentos. Ex.: *gastei 45,90 no mercado via cartão hoje*")
                return {"ok": True}

            # Pergunta e-mail
            pending[chat_id_str] = {"step": "ask_email", "license_key": token}
            await tg_send(chat_id, "Licença ok ✅\nAgora me diga seu e-mail (ex.: cliente@gmail.com).")
            return {"ok": True}

        # Fluxo guiado
        pending[chat_id_str] = {"step": "ask_license"}
        await tg_send(chat_id,
                      "Olá! 👋\nPor favor, *informe sua licença* (ex.: GF–ABCD–1234).\n\n"
                      "Você pode digitar */cancel* para cancelar.")
        return {"ok": True}

    # ===== Passos do fluxo guiado =====
    state = pending.get(chat_id_str)
    if state:
        # Passo 1: receber licença
        if state.get("step") == "ask_license":
            token = text.strip().upper()
            lic = get_license(token)
            ok, err = is_license_valid(lic)
            if not ok:
                await tg_send(chat_id, f"❌ Licença inválida: {err}\nTente novamente ou digite /cancel.")
                return {"ok": True}
            ok2, err2 = bind_license_to_chat(chat_id_str, token)
            if not ok2:
                await tg_send(chat_id, f"❌ {err2}")
                return {"ok": True}
            pending[chat_id_str] = {"step": "ask_email", "license_key": token}
            await tg_send(chat_id, "Licença ok ✅\nAgora me diga seu e-mail (ex.: cliente@gmail.com).")
            return {"ok": True}

        # Passo 2: receber e-mail
        if state.get("step") == "ask_email":
            email = text.strip()
            if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
                await tg_send(chat_id, "E-mail inválido. Tente novamente (ex.: cliente@gmail.com) ou digite /cancel.")
                return {"ok": True}
            await tg_send(chat_id, "✅ Obrigado! Configurando sua planilha de lançamentos...")
            okf, errf, share = await setup_client_file(chat_id_str, email)
            if not okf:
                await tg_send(chat_id, f"❌ {errf}")
                reset_pending(chat_id_str)
                return {"ok": True}

            if EMAIL_SEND_ENABLED and share:
                try:
                    send_mail(
                        subject="Sua planilha de lançamentos está pronta",
                        body_text=f"Olá!\n\nSua planilha foi criada com sucesso. Acesse: {share}\n",
                        to_email=email
                    )
                    await tg_send(chat_id, f"✉️ E-mail enviado para `{email}`")
                except Exception as e:
                    await tg_send(chat_id, f"⚠️ Planilha criada, mas falhou enviar e-mail: {e}")

            await tg_send(chat_id, "🚀 Planilha configurada com sucesso!\n\nTudo certo! Agora pode me contar seus gastos/recebimentos. Ex.: *gastei 45,90 no mercado via cartão hoje*")
            reset_pending(chat_id_str)
            return {"ok": True}

    # ===== exige licença para qualquer uso (se habilitado) =====
    if LICENSE_ENFORCE:
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
        excel_add_row(row, chat_id_str)
        await tg_send(chat_id, "✅ Lançado!")
    except Exception as e:
        await tg_send(chat_id, f"❌ Erro ao lançar na planilha: {e}")

    return {"ok": True}
