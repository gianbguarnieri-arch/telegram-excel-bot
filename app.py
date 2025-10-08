import os
from datetime import datetime
from fastapi import FastAPI, Request
import requests
import httpx
import msal

# =========================
# Configuração do aplicativo
# =========================
app = FastAPI()

# ====== ENVs (alinhadas ao seu Render) ======
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Modelo a ser copiado e pasta de destino
TEMPLATE_FILE_PATH = os.getenv("TEMPLATE_FILE_PATH")  # ex: /users/SEU_UPN/drive/items/<ID>
DEST_FOLDER_ITEM_ID = os.getenv("DEST_FOLDER_ITEM_ID")  # ex: 01HHV...

# Planilha de clientes (onde registramos chat_id e link)
# Aceita: (a) /users/.../drive/items/<ID> (b) apenas o ID (c) /users/.../drive/root:/path.xlsx
CLIENTS_TABLE_PATH = os.getenv("CLIENTS_TABLE_PATH")  # ex: /users/SEU_UPN/drive/items/<ID do Clientes.xlsx> OU apenas <ID>
CLIENTS_WORKSHEET_NAME = os.getenv("CLIENTS_WORKSHEET_NAME", "Plan1")
CLIENTS_TABLE_NAME = os.getenv("CLIENTS_TABLE_NAME", "Clientes")

# Debug opcional
DEBUG = os.getenv("DEBUG", "0") == "1"

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SCOPE = ["https://graph.microsoft.com/.default"]


# =========================
# Autenticação no Graph
# =========================
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


def _users_prefix_from_template():
    """
    Se TEMPLATE_FILE_PATH começa com /users/<upn>/drive, reaproveita esse prefixo;
    caso contrário, usa /me/drive (fallback).
    """
    tfp = (TEMPLATE_FILE_PATH or "").strip()
    if tfp.startswith("/users/"):
        return tfp.split("/drive")[0] + "/drive"
    return "/me/drive"


def normalize_drive_path(raw_path: str) -> str:
    """
    Normaliza CLIENTS_TABLE_PATH (ou outros caminhos) para um formato aceito:
    - Se vier só o ID (ex.: "01HHV..."), vira "/users/<upn>/drive/items/<ID>"
    - Se já vier "/users/.../drive/items/<ID>" ou "/users/.../drive/root:...", mantém
    - Remove espaços/quebras de linha invisíveis
    - Não aceita link http(s)
    """
    if not raw_path:
        raise RuntimeError("CLIENTS_TABLE_PATH não definido.")

    p = raw_path.strip()

    # Não pode ser URL http(s) aqui
    if p.lower().startswith("http"):
        raise RuntimeError("CLIENTS_TABLE_PATH não deve ser um link http(s). Use items ID ou caminho do Graph (/users/...).")

    # Já está no formato /users/.../drive/...
    if p.startswith("/users/"):
        return p

    # Se o valor é um ID de item (muito comum começar com '01')
    if p.startswith("01"):
        upn_drive = _users_prefix_from_template()
        return f"{upn_drive}/items/{p}"

    # Se começar com /drive/... (sem o /users/<upn>), prefixamos com o mesmo UPN do template
    if p.startswith("/drive/"):
        upn_prefix = _users_prefix_from_template().split("/drive")[0]
        return f"{upn_prefix}{p}"

    # Estilo root:
    upn_drive = _users_prefix_from_template()
    if p.startswith("root:") or p.startswith("/root:") or p.startswith("/drive/root:"):
        suffix = p if p.startswith("/drive/") else f"/{p.lstrip('/')}"
        return f"{upn_drive}{suffix}"

    # fallback final: assume arquivo diretamente sob root:
    return f"{upn_drive}/root:/{p.lstrip('/')}"


# ============================================
# Compartilhamento automático da planilha
# ============================================
def create_share_link_anyone_edit(item_id: str):
    """Tenta criar link 'anonymous edit'. Se 403/bloqueado, tenta 'organization edit'. Se falhar, retorna None."""
    token = msal_token()

    # 1) tenta 'anonymous' (qualquer pessoa com o link)
    url = f"{GRAPH_BASE}/me/drive/items/{item_id}/createLink"
    body = {"type": "edit", "scope": "anonymous"}
    r = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=body, timeout=30)
    if DEBUG:
        print("DEBUG createLink anonymous status:", r.status_code, r.text[:300])

    if r.status_code < 300:
        try:
            return r.json()["link"]["webUrl"]
        except Exception:
            pass

    # 2) se não der, tenta 'organization' (usuários logados no seu tenant)
    body = {"type": "edit", "scope": "organization"}
    r2 = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=body, timeout=30)
    if DEBUG:
        print("DEBUG createLink organization status:", r2.status_code, r2.text[:300])

    if r2.status_code < 300:
        try:
            return r2.json()["link"]["webUrl"]
        except Exception:
            pass

    # 3) fallback: sem link de compartilhamento
    return None


# ============================================
# Onboarding: copiar modelo, compartilhar e registrar cliente
# ============================================
def create_client_copy(chat_id, username):
    token = msal_token()
    upn_drive = _users_prefix_from_template()

    # Monta o endpoint /copy corretamente (por items vs por caminho)
    tfp = (TEMPLATE_FILE_PATH or "").strip()
    if "/drive/items/" in tfp or tfp.startswith("/users/"):
        copy_url = f"{GRAPH_BASE}{tfp}/copy" if "/drive/items/" in tfp else f"{GRAPH_BASE}{tfp}:/copy"
    else:
        tfp_norm = normalize_drive_path(tfp)
        copy_url = f"{GRAPH_BASE}{tfp_norm}/copy" if "/drive/items/" in tfp_norm else f"{GRAPH_BASE}{tfp_norm}:/copy"

    safe_user = (username or str(chat_id)).strip().replace(" ", "_")
    copy_name = f"Planilha_{safe_user}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    body = {
        "parentReference": {"id": DEST_FOLDER_ITEM_ID},
        "name": copy_name,
    }

    r = requests.post(
        copy_url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=body,
        timeout=30,
    )

    if DEBUG:
        print("DEBUG /copy URL:", copy_url)
        print("DEBUG /copy status:", r.status_code, r.text[:400])

    if r.status_code not in (202, 200, 201):
        raise RuntimeError(f"Erro ao copiar modelo ({r.status_code}): {r.text}")

    # Quando 202, a resposta retorna um Location para acompanhar
    location = r.headers.get("Location")
    new_item_id = None
    web_url = None

    # Polling simples (até ~20s) aguardando a cópia
    if location:
        for _ in range(40):
            rr = requests.get(location, headers={"Authorization": f"Bearer {token}"}, timeout=30)
            if rr.status_code in (200, 201):
                try:
                    data = rr.json()
                except Exception:
                    data = {}
                new_item_id = data.get("id") or data.get("resourceId") or data.get("resource", {}).get("id")
                web_url = data.get("webUrl") or data.get("resource", {}).get("webUrl")
                if new_item_id:
                    break
            elif rr.status_code in (202, 303):
                # ainda processando
                pass
            else:
                if DEBUG:
                    print("DEBUG polling unexpected:", rr.status_code, rr.text[:400])
                break

    # Fallback: se não veio ID, lista a pasta de destino e encontra pelo nome
    if not new_item_id:
        children_url = f"{GRAPH_BASE}{upn_drive}/items/{DEST_FOLDER_ITEM_ID}/children"
        rr = requests.get(children_url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
        if rr.status_code < 300:
            for it in rr.json().get("value", []):
                if it.get("name") == copy_name:
                    new_item_id = it.get("id")
                    web_url = it.get("webUrl")
                    break

    if not new_item_id:
        raise RuntimeError("Não consegui obter o ID do arquivo copiado (tente novamente).")

    # Tenta gerar link de compartilhamento (anonymous -> organization -> fallback)
    share_url = create_share_link_anyone_edit(new_item_id)
    final_url = share_url or web_url or "(sem link)"

    # Caminho do arquivo copiado em formato /users/.../drive/items/<id>
    excel_path_items = f"{upn_drive}/items/{new_item_id}"

    # Registra o cliente na planilha Clientes
    register_client(chat_id, username, final_url, excel_path_items)

    return final_url


def register_client(chat_id, username, planilha_url, excel_path_items):
    token = msal_token()

    # Normaliza o caminho informado na ENV (aceita só ID, items, root: etc.)
    clients_path = normalize_drive_path(CLIENTS_TABLE_PATH)

    # Suporte a path por items (/drive/items/<ID>) ou por caminho (:/workbook)
    if "/drive/items/" in clients_path:
        url = (
            f"{GRAPH_BASE}{clients_path}"
            f"/workbook/worksheets('{CLIENTS_WORKSHEET_NAME}')"
            f"/tables('{CLIENTS_TABLE_NAME}')/rows/add"
        )
    else:
        url = (
            f"{GRAPH_BASE}{clients_path}"
            f":/workbook/worksheets('{CLIENTS_WORKSHEET_NAME}')"
            f"/tables('{CLIENTS_TABLE_NAME}')/rows/add"
        )

    # chat_id | username | planilha_url(compartilhável se possível) | excel_path_items | created_at
    values = [[
        str(chat_id),
        (username or "-"),
        planilha_url,
        excel_path_items,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    ]]

    if DEBUG:
        print("DEBUG register URL:", url)
        print("DEBUG register VALUES:", values)

    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        json={"values": values},
        timeout=30,
    )

    if DEBUG:
        print("DEBUG register status:", r.status_code, r.text[:400])

    if r.status_code >= 300:
        raise RuntimeError(f"Erro ao registrar cliente: {r.text}")


# =========================
# Telegram helpers
# =========================
async def tg_send(chat_id, text):
    async with httpx.AsyncClient(timeout=15) as client:
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text},
        )


# =========================
# Rotas
# =========================
@app.get("/")
def root():
    return {"status": "ok"}


@app.post("/telegram/webhook")
async def telegram_webhook(req: Request):
    body = await req.json()
    message = body.get("message", {})
    chat = message.get("chat", {})
    chat_id = chat.get("id")
    username = chat.get("first_name") or chat.get("username")
    text = (message.get("text") or "").strip()

    if not chat_id or not text:
        return {"ok": True}

    if text.lower().startswith("/start"):
        try:
            await tg_send(chat_id, "🕐 Criando sua planilha personalizada, aguarde alguns segundos...")
            web_url = create_client_copy(chat_id, username)
            await tg_send(
                chat_id,
                "✅ Sua planilha foi criada!\n\n"
                "📂 Acesse aqui (compartilhável):\n"
                f"{web_url}\n\n"
                "Se precisar, eu compartilho o arquivo pra você."
            )
        except Exception as e:
            await tg_send(chat_id, f"❌ Erro ao criar a planilha: {e}")
        return {"ok": True}

    await tg_send(chat_id, "Envie /start para gerar sua planilha personalizada.")
    return {"ok": True}
