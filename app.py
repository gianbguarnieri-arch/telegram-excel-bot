import os
import re
from datetime import datetime
from fastapi import FastAPI, Request
import requests
import httpx
import msal

app = FastAPI()

# ===== Variáveis de ambiente =====
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TEMPLATE_FILE_PATH = os.getenv("TEMPLATE_FILE_PATH")
DEST_FOLDER_ITEM_ID = os.getenv("DEST_FOLDER_ITEM_ID")
CLIENTS_FILE_PATH = os.getenv("CLIENTS_FILE_PATH")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Plan1")
TABLE_NAME = os.getenv("TABLE_NAME", "Clientes")
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SCOPE = ["https://graph.microsoft.com/.default"]

# ===== Autenticação Microsoft Graph =====
def msal_token():
    app_msal = msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        client_credential=CLIENT_SECRET,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}"
    )
    result = app_msal.acquire_token_silent(SCOPE, account=None)
    if not result:
        result = app_msal.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"MSAL error: {result}")
    return result["access_token"]

# ===== Funções de integração com Graph =====
def _users_prefix_from_template():
    # Se TEMPLATE_FILE_PATH começa com /users/<UPN>/drive, reaproveita esse prefixo.
    # Caso contrário, usa /me/drive como fallback.
    if TEMPLATE_FILE_PATH and TEMPLATE_FILE_PATH.startswith("/users/"):
        return TEMPLATE_FILE_PATH.split("/drive")[0] + "/drive"
    return "/me/drive"

def create_client_copy(chat_id, username):
    token = msal_token()
    upn_drive = _users_prefix_from_template()

    # Monta URL do /copy corretamente (path por items)
    if "/drive/items/" in TEMPLATE_FILE_PATH:
        copy_url = f"{GRAPH_BASE}{TEMPLATE_FILE_PATH}/copy"
    else:
        copy_url = f"{GRAPH_BASE}{TEMPLATE_FILE_PATH}:/copy"

    # Nome da cópia (sem espaços estranhos)
    safe_user = (username or str(chat_id)).strip().replace(" ", "_")
    copy_name = f"Planilha_{safe_user}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    body = {
        "parentReference": {"id": DEST_FOLDER_ITEM_ID},
        "name": copy_name
    }

    r = requests.post(
        copy_url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body,
        timeout=30
    )
    if r.status_code not in (202, 200, 201):
        raise RuntimeError(f"Erro ao copiar modelo ({r.status_code}): {r.text}")

    # A API geralmente retorna 202 + Location para acompanhar
    location = r.headers.get("Location")
    new_item_id = None
    web_url = None

    # Polling simples (até ~20s) para concluir a cópia
    if location:
        for _ in range(40):
            rr = requests.get(location, headers={"Authorization": f"Bearer {token}"}, timeout=30)
            if rr.status_code in (200, 201):
                try:
                    data = rr.json()
                except Exception:
                    data = {}
                new_item_id = data.get("id") or data.get("resourceId") or data.get("resource", {}).get("id")
                web_url = (data.get("webUrl") or data.get("resource", {}).get("webUrl"))
                if new_item_id:
                    break
            elif rr.status_code in (202, 303):
                # ainda processando
                pass
            else:
                break

    # Fallback: se não veio o ID, lista a pasta de destino e acha pelo nome
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

    # Monta EXCEL_PATH por items (mais estável)
    excel_path_items = f"{upn_drive}/items/{new_item_id}"

    # Registra o cliente na planilha de Clientes
    register_client(chat_id, username, web_url or "(sem link)", excel_path_items)

    return web_url or "(sem link)"

def register_client(chat_id, username, planilha_url, excel_path_items):
    token = msal_token()

    # Suporta tanto caminho por items quanto por caminho textual
    if "/drive/items/" in CLIENTS_FILE_PATH:
        url = f"{GRAPH_BASE}{CLIENTS_FILE_PATH}/workbook/worksheets('{WORKSHEET_NAME}')/tables('{TABLE_NAME}')/rows/add"
    else:
        url = f"{GRAPH_BASE}{CLIENTS_FILE_PATH}:/workbook/worksheets('{WORKSHEET_NAME}')/tables('{TABLE_NAME}')/rows/add"

    # Registre: chat_id | username | planilha_url | excel_path_items | created_at
    values = [[
        str(chat_id),
        (username or "-"),
        planilha_url,
        excel_path_items,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ]]
    r = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json={"values": values}, timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"Erro ao registrar cliente: {r.text}")


def register_client(chat_id, username, planilha_url):
    token = msal_token()
    url = (
        f"{GRAPH_BASE}{CLIENTS_FILE_PATH}"
        f":/workbook/worksheets('{WORKSHEET_NAME}')/tables('{TABLE_NAME}')/rows/add"
    )

    values = [[str(chat_id), username or "-", planilha_url, datetime.now().strftime("%Y-%m-%d %H:%M:%S")]]
    r = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json={"values": values})
    if r.status_code >= 300:
        raise RuntimeError(f"Erro ao registrar cliente: {r.text}")

# ===== Telegram =====
async def tg_send(chat_id, text):
    async with httpx.AsyncClient(timeout=15) as client:
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text},
        )

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
    text = (message.get("text") or "").strip().lower()

    if not chat_id or not text:
        return {"ok": True}

    # Comando /start
    if text.startswith("/start"):
        try:
            await tg_send(chat_id, "🕐 Criando sua planilha personalizada, aguarde alguns segundos...")
            web_url = create_client_copy(chat_id, username)
            await tg_send(chat_id, f"✅ Sua planilha foi criada com sucesso!\n\n📂 Acesse aqui:\n{web_url}")
        except Exception as e:
            await tg_send(chat_id, f"❌ Erro ao criar a planilha: {e}")
        return {"ok": True}

    # Qualquer outro texto
    await tg_send(chat_id, "Envie /start para gerar sua planilha personalizada.")
    return {"ok": True}
