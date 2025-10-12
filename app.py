import os
import re
import sqlite3
import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict

import requests
import httpx
import msal
from fastapi import FastAPI, Request, Header

# =========================
# FASTAPI + ENV
# =========================
app = FastAPI()

# Telegram
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET")

# SQLite (Render Free => /tmp)
SQLITE_PATH = os.getenv("SQLITE_PATH", "/tmp/db.sqlite")

# Graph
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Cópia de arquivo (template → pasta de destino)
DRIVE_ID = os.getenv("DRIVE_ID")
TEMPLATE_ITEM_ID = os.getenv("TEMPLATE_ITEM_ID")
DEST_FOLDER_ITEM_ID = os.getenv("DEST_FOLDER_ITEM_ID")

# Excel (worksheet/table no template)
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Plan1")
TABLE_NAME = os.getenv("TABLE_NAME", "Lancamentos")

# Compartilhamento
SHARE_LINK_TYPE = os.getenv("SHARE_LINK_TYPE", "edit")           # edit | view
SHARE_LINK_SCOPE = os.getenv("SHARE_LINK_SCOPE", "anonymous")    # anonymous | organization
SHARE_LINK_PASSWORD = os.getenv("SHARE_LINK_PASSWORD") or None

# E-mail
EMAIL_SEND_ENABLED = os.getenv("EMAIL_SEND_ENABLED", "0") == "1"
MAIL_SENDER_UPN = os.getenv("MAIL_SENDER_UPN")  # UPN do remetente com mailbox

# =========================
# ESTADO EFÊMERO DE CONVERSA
# =========================
# chat_id -> { step: "ask_license"|"ask_email", "license_key": str }
pending: Dict[str, Dict] = {}

def reset_pending(chat_id: str):
    pending.pop(str(chat_id), None)

# =========================
# BANCO
# =========================
def _db():
    return sqlite3.connect(SQLITE_PATH)

def db_init():
    con = _db()
    con.execute("""
    CREATE TABLE IF NOT EXISTS licenses (
        license_key TEXT PRIMARY KEY,
        email TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        expires_at TEXT
    )""")
    con.execute("""
    CREATE TABLE IF NOT EXISTS clients (
        chat_id TEXT PRIMARY KEY,
        email TEXT NOT NULL,
        drive_id TEXT,
        item_id TEXT,
        created_at TEXT,
        last_seen_at TEXT
    )""")
    con.commit(); con.close()

@app.on_event("startup")
def _auto_init_db():
    try:
        os.makedirs(os.path.dirname(SQLITE_PATH), exist_ok=True)
    except Exception:
        pass
    db_init()
    print(f"✅ DB pronto em {SQLITE_PATH}")

def upsert_client(chat_id: str, email: str, drive_id: Optional[str], item_id: Optional[str]):
    con = _db()
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    con.execute("INSERT OR IGNORE INTO clients(chat_id, email, created_at, last_seen_at) VALUES(?,?,?,?)",
                (chat_id, email.lower().strip(), now, now))
    con.execute("UPDATE clients SET email=?, drive_id=?, item_id=?, last_seen_at=? WHERE chat_id=?",
                (email.lower().strip(), drive_id, item_id, now, chat_id))
    con.commit(); con.close()

def get_client(chat_id: str):
    con = _db()
    cur = con.execute("SELECT chat_id,email,drive_id,item_id,created_at,last_seen_at FROM clients WHERE chat_id=?",
                      (chat_id,))
    row = cur.fetchone(); con.close()
    if not row: return None
    return {"chat_id": row[0], "email": row[1], "drive_id": row[2], "item_id": row[3],
            "created_at": row[4], "last_seen_at": row[5]}

def create_license(code: str, email: str, days: int = 7):
    exp = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat(timespec="seconds")
    con = _db()
    con.execute("""
        INSERT OR REPLACE INTO licenses(license_key, email, status, expires_at)
        VALUES (?, ?, 'active', ?)
    """, (code.upper().strip(), email.lower().strip(), exp))
    con.commit(); con.close()

def get_license(code: str):
    con = _db()
    cur = con.execute("SELECT license_key,email,status,expires_at FROM licenses WHERE license_key=?",
                      (code.upper().strip(),))
    row = cur.fetchone(); con.close()
    if not row: return None
    return {"license_key": row[0], "email": row[1], "status": row[2], "expires_at": row[3]}

def is_license_valid_for_email(code: str, email: str):
    lic = get_license(code)
    if not lic: return False, "Licença não encontrada."
    if lic["status"] != "active": return False, "Licença inativa."
    if lic["email"].lower().strip() != email.lower().strip():
        return False, "E-mail não corresponde à licença."
    if lic["expires_at"]:
        try:
            if datetime.now(timezone.utc) > datetime.fromisoformat(lic["expires_at"]):
                return False, "Licença expirada."
        except Exception:
            return False, "Validade inválida."
    return True, None

# =========================
# TELEGRAM
# =========================
async def tg_send(chat_id, text):
    async with httpx.AsyncClient(timeout=15) as client:
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
        )

# =========================
# GRAPH AUTH
# =========================
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

# =========================
# GRAPH: itens / cópia
# =========================
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

async def graph_copy_file(template_item_id: str, drive_id: str, dest_folder_id: str, new_file_name: str) -> Optional[str]:
    if not all([template_item_id, drive_id, dest_folder_id]):
        raise RuntimeError("DRIVE_ID, TEMPLATE_ITEM_ID, DEST_FOLDER_ITEM_ID precisam estar configurados.")
    token = msal_token()
    source_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{template_item_id}/copy"

    async def _try_copy(_name: str) -> Optional[str]:
        payload = {"parentReference": {"driveId": drive_id, "id": dest_folder_id}, "name": _name}
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(source_url, headers={"Authorization": f"Bearer {token}"}, json=payload)

        if r.status_code == 202:
            # polling por nome
            safe = _name.replace("'", "''")
            search_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{dest_folder_id}/children"
            for _ in range(30):  # ~60s
                async with httpx.AsyncClient(timeout=15) as client:
                    sr = await client.get(
                        search_url,
                        headers={"Authorization": f"Bearer {token}"},
                        params={"$filter": f"name eq '{safe}'"}
                    )
                if sr.status_code < 300:
                    data = sr.json()
                    if data.get("value"):
                        return data["value"][0]["id"]
                await asyncio.sleep(2)
            return None

        if r.status_code == 409:
            return "NAME_CONFLICT"

        raise RuntimeError(f"Erro na cópia: {r.status_code} {r.text}")

    res = await _try_copy(new_file_name)
    if res == "NAME_CONFLICT":
        base = re.sub(r"\.xlsx$", "", new_file_name, flags=re.I)
        unique = datetime.now().strftime("-%Y%m%d-%H%M%S")
        new_name = f"{base}{unique}.xlsx"
        res = await _try_copy(new_name)

    if not res:
        raise RuntimeError("Timeout aguardando a planilha copiada aparecer.")
    if res == "NAME_CONFLICT":
        raise RuntimeError("Conflito de nome persistente.")
    return res

# =========================
# GRAPH: permissões / link
# =========================
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

def graph_create_share_link(drive_id: str, item_id: str,
                            link_type: Optional[str] = None,
                            scope: Optional[str] = None,
                            password: Optional[str] = None) -> str:
    graph_wait_item_available(drive_id, item_id, timeout_sec=25)
    # remove links antigos
    try:
        perms = graph_list_permissions(drive_id, item_id)
        for p in perms:
            if p.get("link"):
                graph_delete_permission(drive_id, item_id, p["id"])
    except Exception:
        pass

    token = msal_token()
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/createLink"
    payload = {"type": (link_type or SHARE_LINK_TYPE), "scope": (scope or SHARE_LINK_SCOPE)}
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
    raise RuntimeError(f"createLink error 404 after retries: {last_err}")

# =========================
# GRAPH: e-mail
# =========================
def graph_send_mail(to_email: str, subject: str, html_body: str):
    if not EMAIL_SEND_ENABLED:
        return
    if not MAIL_SENDER_UPN:
        raise RuntimeError("MAIL_SENDER_UPN não configurado.")
    token = msal_token()
    url = f"{GRAPH_BASE}/users/{MAIL_SENDER_UPN}/sendMail"
    msg = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": to_email}}],
        },
        "saveToSentItems": True
    }
    r = requests.post(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                      json=msg, timeout=20)
    if r.status_code not in (200, 202):
        raise RuntimeError(f"sendMail error {r.status_code}: {r.text}")

# =========================
# PROVISIONAMENTO
# =========================
async def provision_for_client(chat_id: str, email: str) -> Tuple[bool, Optional[str], Optional[str]]:
    safe_email = re.sub(r"[^A-Za-z0-9._@+-]", "_", email.strip())
    file_name = f"Lancamentos - {safe_email}.xlsx"

    # Reuso se já existir
    try:
        token = msal_token()
        safe = file_name.replace("'", "''")
        search_url = f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{DEST_FOLDER_ITEM_ID}/children?$filter=name eq '{safe}'"
        r = requests.get(search_url, headers={"Authorization": f"Bearer {token}"}, timeout=15)
        if r.status_code < 300:
            data = r.json()
            if data.get("value"):
                item_id = data["value"][0]["id"]
                upsert_client(chat_id, email, DRIVE_ID, item_id)
                share_url = graph_create_share_link(DRIVE_ID, item_id)
                return True, None, share_url
    except Exception:
        pass

    # Copia template
    try:
        new_item_id = await graph_copy_file(TEMPLATE_ITEM_ID, DRIVE_ID, DEST_FOLDER_ITEM_ID, file_name)
    except Exception as e:
        return False, f"Falha ao criar planilha: {e}", None

    # Link
    try:
        share_url = graph_create_share_link(DRIVE_ID, new_item_id)
    except Exception as e:
        upsert_client(chat_id, email, DRIVE_ID, new_item_id)
        return True, f"Planilha criada, mas falhou ao gerar link: {e}", None

    upsert_client(chat_id, email, DRIVE_ID, new_item_id)

    # E-mail
    try:
        if EMAIL_SEND_ENABLED:
            extra_pwd = f"<p>Senha do link: <b>{SHARE_LINK_PASSWORD}</b></p>" if (SHARE_LINK_PASSWORD and SHARE_LINK_SCOPE == "anonymous") else ""
            html = f"""
            <p>Olá!</p>
            <p>Sua planilha de lançamentos foi criada com sucesso.</p>
            <p>Acesse: <a href="{share_url}">{share_url}</a></p>
            {extra_pwd}
            """
            graph_send_mail(email, "Sua planilha de lançamentos está pronta", html)
    except Exception:
        pass

    return True, None, share_url

# =========================
# NLP simples + lançamento
# =========================
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
    "Mercado": ["mercado", "supermercado", "rancho", "hortifruti"],
    "Farmácia": ["farmácia", "remédio", "medicamento", "drogaria"],
    "Combustível": ["gasolina", "álcool", "etanol", "diesel", "posto", "combustível"],
    "Ifood": ["ifood", "i-food"],
    "Passeio em família": ["passeio", "parque", "cinema", "lazer"],
    "Viagem": ["hotel", "passagem", "viagem", "airbnb"],
    "Assinatura": ["netflix", "amazon", "disney", "spotify", "premiere"],
    "Aluguel": ["aluguel", "condomínio"], "Água": ["água"], "Energia": ["energia","luz"],
    "Internet": ["internet","fibra"], "Escola": ["escola","mensalidade"], "Imposto": ["iptu","ipva"]
}

def map_group(category: str) -> str:
    if category in ["Aluguel","Água","Energia","Internet","Plano de Saúde","Escola","Assinatura"]: return "Gastos Fixos"
    if category in ["Imposto","Financiamento","Empréstimo"]: return "Despesas Temporárias"
    return "Gastos Variáveis"

def detect_category_and_desc(text: str) -> Tuple[str, Optional[str]]:
    t = text.lower()
    for cat, kws in CATEGORIES.items():
        for kw in kws:
            if kw in t:
                return cat, None
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

def excel_add_row_by_item(drive_id: str, item_id: str, values: List):
    token = msal_token()
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/workbook/worksheets('{WORKSHEET_NAME}')/tables('{TABLE_NAME}')/rows/add"
    r = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json={"values": [values]}, timeout=25)
    if r.status_code >= 300:
        raise RuntimeError(f"Graph error {r.status_code}: {r.text}")

# =========================
# ROTEAMENTO TELEGRAM
# =========================
@app.post("/telegram/webhook")
async def telegram_webhook(
    req: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(None)
):
    # valida secret do webhook (se configurado)
    if TELEGRAM_WEBHOOK_SECRET and (x_telegram_bot_api_secret_token != TELEGRAM_WEBHOOK_SECRET):
        return {"ok": True}

    body = await req.json()
    message = body.get("message") or {}
    chat_id = message.get("chat", {}).get("id")
    text = (message.get("text") or "").strip()
    low = text.lower()

    if not chat_id or not text:
        return {"ok": True}

    chat_id_str = str(chat_id)

    # ===== ADMIN =====
    if ADMIN_TELEGRAM_ID and chat_id_str == str(ADMIN_TELEGRAM_ID):
        if low.startswith("/licenca nova"):
            parts = text.split()
            # /licenca nova CODIGO EMAIL [DIAS]
            if len(parts) < 4:
                await tg_send(chat_id, "Uso: `/licenca nova CODIGO EMAIL [DIAS]`\nEx.: `/licenca nova GF-ABCD-1234 cliente@gmail.com 7`")
                return {"ok": True}
            code = parts[2].strip().upper()
            email = parts[3].strip()
            days = int(parts[4]) if (len(parts) >= 5 and parts[4].isdigit()) else 7
            try:
                create_license(code, email, days)
                await tg_send(chat_id, f"✅ Licença criada:\n• Chave: `{code}`\n• Email: `{email}`\n• Validade: {days} dia(s)")
            except Exception as e:
                await tg_send(chat_id, f"❌ Erro ao criar licença: {e}")
            return {"ok": True}

        if low.startswith("/db init"):
            try:
                db_init()
                await tg_send(chat_id, f"✅ DB inicializado em `{SQLITE_PATH}`")
            except Exception as e:
                await tg_send(chat_id, f"❌ Erro ao inicializar DB: {e}")
            return {"ok": True}

    # ===== FLUXO AMIGÁVEL =====
    if low.startswith("/start"):
        reset_pending(chat_id_str)
        pending[chat_id_str] = {"step": "ask_license"}
        await tg_send(chat_id, "🔑 *Informe sua licença* (ex.: `GF-ABCD-1234`)\n\nDigite */cancel* para cancelar.")
        return {"ok": True}

    state = pending.get(chat_id_str)
    if state:
        if low.startswith("/cancel"):
            reset_pending(chat_id_str)
            await tg_send(chat_id, "✅ Cancelado. Envie */start* para começar novamente.")
            return {"ok": True}

        if state.get("step") == "ask_license":
            license_key = text.strip().upper()
            lic = get_license(license_key)
            if not lic:
                await tg_send(chat_id, "❌ Licença não encontrada. Tente novamente ou digite */cancel*.")
                return {"ok": True}
            if lic["status"] != "active":
                await tg_send(chat_id, "❌ Licença inativa. Fale com o suporte.")
                return {"ok": True}
            if lic["expires_at"]:
                try:
                    if datetime.now(timezone.utc) > datetime.fromisoformat(lic["expires_at"]):
                        await tg_send(chat_id, "❌ Licença expirada. Fale com o suporte.")
                        return {"ok": True}
                except Exception:
                    await tg_send(chat_id, "❌ Validade da licença inválida. Fale com o suporte.")
                    return {"ok": True}

            pending[chat_id_str] = {"step": "ask_email", "license_key": license_key}
            await tg_send(chat_id, "📧 *Informe seu e-mail* (ex.: `cliente@gmail.com`)")
            return {"ok": True}

        if state.get("step") == "ask_email":
            email = text.strip()
            if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
                await tg_send(chat_id, "❌ E-mail inválido. Tente novamente (ex.: `cliente@gmail.com`) ou digite */cancel*.")
                return {"ok": True}

            license_key = state.get("license_key")
            ok, err = is_license_valid_for_email(license_key, email)
            if not ok:
                await tg_send(chat_id, f"❌ {err}")
                return {"ok": True}

            await tg_send(chat_id, "⏳ Configurando sua planilha…")
            ok2, err2, share = await provision_for_client(chat_id_str, email)
            reset_pending(chat_id_str)

            if not ok2:
                await tg_send(chat_id, f"❌ {err2}")
                return {"ok": True}

            if share:
                await tg_send(chat_id, f"🚀 *Sua planilha foi criada!*\n\nAcesse:\n{share}\n\nAgora é só me mandar mensagens como:\n_\"gastei 45,90 no mercado via pix hoje\"_ que eu lanço pra você 😉")
            else:
                await tg_send(chat_id, "🚀 *Sua planilha foi criada!* (não consegui gerar o link agora).")
            return {"ok": True}

    # ===== LANÇAMENTOS (modo uso) =====
    # Se não é comando e não está no fluxo pendente → tenta lançar
    if not low.startswith("/"):
        cli = get_client(chat_id_str)
        if not cli or not cli.get("item_id") or not cli.get("drive_id"):
            await tg_send(chat_id, "Para começar, envie */start* e faça a configuração inicial.")
            return {"ok": True}

        row, perr = parse_natural(text)
        if perr:
            await tg_send(chat_id, f"❗ {perr}\nEx.: _gastei 45,90 no mercado via pix hoje_")
            return {"ok": True}

        try:
            excel_add_row_by_item(cli["drive_id"], cli["item_id"], row)
            await tg_send(chat_id, "✅ Lançado!")
        except Exception as e:
            await tg_send(chat_id, f"❌ Erro ao lançar na planilha: {e}")
        return {"ok": True}

    # ===== fallback =====
    await tg_send(chat_id, "Use */start* para configurar ou me envie um lançamento, ex.: _gastei 45,90 no mercado via pix hoje_.")
    return {"ok": True}

@app.get("/")
def health():
    return {"status": "ok"}
