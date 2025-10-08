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
def create_client_copy(chat_id, username):
    token = msal_token()
    copy_name = f"Planilha_{username or chat_id}_{datetime.now().strftime('%d%m%Y_%H%M')}.xlsx"
    copy_url = f"{GRAPH_BASE}{TEMPLATE_FILE_PATH}/copy"

    body = {
        "parentReference": {"id": DEST_FOLDER_ITEM_ID},
        "name": copy_name
    }

    r = requests.post(copy_url, headers={"Authorization": f"Bearer {token}"}, json=body)
    if r.status_code >= 300:
        raise RuntimeError(f"Erro ao copiar modelo: {r.text}")

    # Pega o link da cópia criada
    new_file = r.json()
    web_url = new_file.get("webUrl", "Link não disponível")

    # Registra o cliente na planilha
    register_client(chat_id, username, web_url)
    return web_url

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
