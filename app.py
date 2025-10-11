import os
import re
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

# =========================
# FASTAPI + ENV
# =========================
app = FastAPI()

# Telegram
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET")

# SQLite
SQLITE_PATH = os.getenv("SQLITE_PATH", "/tmp/licenses.db")

# Graph
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Cópia de arquivo
DRIVE_ID = os.getenv("DRIVE_ID")
TEMPLATE_ITEM_ID = os.getenv("TEMPLATE_ITEM_ID")
DEST_FOLDER_ITEM_ID = os.getenv("DEST_FOLDER_ITEM_ID")

# Excel (tabela dentro do template)
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Plan1")
TABLE_NAME = os.getenv("TABLE_NAME", "Lancamentos")

# Compartilhamento
SHARE_LINK_TYPE = os.getenv("SHARE_LINK_TYPE", "edit")           # edit | view
SHARE_LINK_SCOPE = os.getenv("SHARE_LINK_SCOPE", "anonymous")    # anonymous | organization
SHARE_LINK_PASSWORD = os.getenv("SHARE_LINK_PASSWORD") or None

# E-mail
EMAIL_SEND_ENABLED = os.getenv("EMAIL_SEND_ENABLED", "0") == "1"
MAIL_SENDER_UPN = os.getenv("MAIL_SENDER_UPN")  # UPN do remetente (conta da org com mailbox)

# =========================
# BANCO (licenses)
# =========================
def licenses_db_init():
    con = sqlite3.connect(SQLITE_PATH)
    con.execute("""
    CREATE TABLE IF NOT EXISTS licenses (
        license_key TEXT PRIMARY KEY,
        email TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        expires_at TEXT
    )
    """)
    con.commit()
    con.close()

@app.on_event("startup")
def _auto_init_db():
    try:
        dbdir = os.path.dirname(SQLITE_PATH)
        if dbdir:
            os.makedirs(dbdir, exist_ok=True)
    except Exception:
        pass
    try:
        licenses_db_init()
        print(f"✅ DB inicializado em {SQLITE_PATH}")
    except Exception as e:
        print(f"❌ Erro ao inicializar DB: {e}")

def create_license(code: str, email: str, days: int = 7):
    exp = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat(timespec="seconds")
    con = sqlite3.connect(SQLITE_PATH)
    con.execute("""
        INSERT OR REPLACE INTO licenses(license_key, email, status, expires_at)
        VALUES (?, ?, 'active', ?)
    """, (code.upper().strip(), email.lower().strip(), exp))
    con.commit()
    con.close()

def get_license(code: str):
    con = sqlite3.connect(SQLITE_PATH)
    cur = con.cursor()
    cur.execute("SELECT license_key, email, status, expires_at FROM licenses WHERE license_key=?",
                (code.upper().strip(),))
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    return {"license_key": row[0], "email": row[1], "status": row[2], "expires_at": row[3]}

def is_license_valid(code: str, email: str):
    lic = get_license(code)
    if not lic:
        return False, "Licença não encontrada."
    if lic["status"] != "active":
        return False, "Licença inativa."
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
# GRAPH: helpers (itens)
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
    """
    Copia o arquivo modelo e retorna o ID do novo arquivo (item_id).
    Faz polling até ~60s para o item aparecer; resolve 409 com sufixo único.
    """
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
# GRAPH: permissions / links
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
    # aguarda visibilidade
    graph_wait_item_available(drive_id, item_id, timeout_sec=25)

    # remove links antigos (evita reuso)
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
    for attempt in range(6):  # retry 404 com backoff
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
# GRAPH: enviar e-mail
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
# EXCEL: inserir linha (se necessário no futuro)
# =========================
def _build_workbook_rows_add_url(excel_path: str) -> str:
    if "/drive/items/" in excel_path:
        return f"{GRAPH_BASE}{excel_path}/workbook/worksheets('{WORKSHEET_NAME}')/tables('{TABLE_NAME}')/rows/add"
    else:
        return f"{GRAPH_BASE}{excel_path}:/workbook/worksheets('{WORKSHEET_NAME}')/tables('{TABLE_NAME}')/rows/add"

def excel_path_for_item(drive_id: str, item_id: str) -> str:
    return f"/drives/{drive_id}/items/{item_id}"

# =========================
# FUNÇÃO: criar planilha do cliente + link + e-mail
# =========================
async def provision_for_client(email: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """Cria a cópia nomeada com o e-mail, gera link e envia e-mail. Retorna (ok, err, share_url)."""
    # nome do arquivo
    safe_email = re.sub(r"[^A-Za-z0-9._@+-]", "_", email.strip())
    file_name = f"Lancamentos - {safe_email}.xlsx"

    # 1) tenta encontrar existente por nome (para reuso)
    try:
        token = msal_token()
        safe = file_name.replace("'", "''")
        search_url = f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{DEST_FOLDER_ITEM_ID}/children?$filter=name eq '{safe}'"
        r = requests.get(search_url, headers={"Authorization": f"Bearer {token}"}, timeout=15)
        if r.status_code < 300:
            data = r.json()
            if data.get("value"):
                item_id = data["value"][0]["id"]
                # gera link novo
                share_url = graph_create_share_link(DRIVE_ID, item_id)
                return True, None, share_url
    except Exception:
        pass

    # 2) copia template
    try:
        new_item_id = await graph_copy_file(TEMPLATE_ITEM_ID, DRIVE_ID, DEST_FOLDER_ITEM_ID, file_name)
    except Exception as e:
        return False, f"Falha ao criar planilha: {e}", None

    # 3) link de compartilhamento
    try:
        share_url = graph_create_share_link(DRIVE_ID, new_item_id)
    except Exception as e:
        return True, f"Planilha criada, mas falhou ao gerar link: {e}", None

    # 4) e-mail ao cliente
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
    except Exception as e:
        # não bloqueia o fluxo por erro de e-mail
        pass

    return True, None, share_url

# =========================
# TELEGRAM WEBHOOK
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

    # ===== ADMIN =====
    if ADMIN_TELEGRAM_ID and str(chat_id) == str(ADMIN_TELEGRAM_ID):
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
                licenses_db_init()
                await tg_send(chat_id, f"✅ DB inicializado em `{SQLITE_PATH}`")
            except Exception as e:
                await tg_send(chat_id, f"❌ Erro ao inicializar DB: {e}")
            return {"ok": True}

    # ===== CLIENTE: /start CODIGO EMAIL =====
    if low.startswith("/start"):
        parts = text.split()
        if len(parts) < 3:
            await tg_send(chat_id, "Uso: `/start CODIGO EMAIL`\nEx.: `/start GF-ABCD-1234 cliente@gmail.com`")
            return {"ok": True}

        code = parts[1].strip().upper()
        email = parts[2].strip()

        ok, err = is_license_valid(code, email)
        if not ok:
            await tg_send(chat_id, f"❌ {err}")
            return {"ok": True}

        await tg_send(chat_id, "✅ Licença válida! Criando sua planilha personalizada…")
        ok2, err2, share = await provision_for_client(email)
        if not ok2:
            await tg_send(chat_id, f"❌ {err2}")
            return {"ok": True}

        if share:
            await tg_send(chat_id, f"🚀 Planilha pronta!\n\nLink de acesso:\n{share}\n\nSe preferir, verifique seu e-mail — enviamos o link por lá também.")
        else:
            await tg_send(chat_id, "🚀 Planilha pronta! (Não foi possível gerar o link agora.)")

        return {"ok": True}

    # fallback
    await tg_send(chat_id, "Comando não reconhecido. Use:\n• `/start CODIGO EMAIL`\n• (admin) `/licenca nova CODIGO EMAIL [DIAS]`")
    return {"ok": True}

@app.get("/")
def health():
    return {"status": "ok"}
