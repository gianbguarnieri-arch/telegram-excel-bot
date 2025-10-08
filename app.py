# app.py
import os
import time
import json
import re
import asyncio
from datetime import datetime

import requests
import msal
from fastapi import FastAPI, Request, BackgroundTasks
import httpx

app = FastAPI()

# ====== ENVs ======
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TENANT_ID = os.getenv("TENANT_ID", "")
CLIENT_ID = os.getenv("CLIENT_ID", "")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "")

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Modelo a ser copiado e pasta destino
TEMPLATE_FILE_PATH = os.getenv("TEMPLATE_FILE_PATH", "").strip()   # ex: /drive/items/01HHVX77QPUSGDU5MVA5C2BW4JKQMBBS2I
DEST_FOLDER_ITEM_ID = os.getenv("DEST_FOLDER_ITEM_ID", "").strip() # ex: 01HHVX77TGAHSB7UJHTBBL2JMSQ7DRTSAN

DEBUG = os.getenv("DEBUG", "0") == "1"
SCOPE = ["https://graph.microsoft.com/.default"]

# ====== MSAL / Graph helpers ======
def get_token() -> str:
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

def g_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def g_post(url: str, token: str, data: dict | None = None):
    if DEBUG:
        print("POST", url, "DATA", data)
    r = requests.post(url, headers=g_headers(token), json=data)
    if r.status_code >= 300 and r.status_code != 202:
        raise RuntimeError(f"Graph POST {r.status_code}: {r.text}")
    return r

def g_get(url: str, token: str):
    if DEBUG:
        print("GET", url)
    r = requests.get(url, headers=g_headers(token))
    if r.status_code >= 300:
        raise RuntimeError(f"Graph GET {r.status_code}: {r.text}")
    return r

# ====== Utils ======
def safe_sleep_from_retry_after(resp, default_seconds=2):
    ra = resp.headers.get("Retry-After")
    try:
        wait = int(ra) if ra is not None else default_seconds
    except ValueError:
        wait = default_seconds
    time.sleep(max(wait, 1))

def list_child_by_name(dest_folder_id: str, name: str, token: str):
    url = f"{GRAPH_BASE}/drive/items/{dest_folder_id}/children?$select=id,name,lastModifiedDateTime&$top=200"
    while True:
        r = g_get(url, token)
        data = r.json()
        for it in data.get("value", []):
            if it.get("name") == name:
                return it
        next_link = data.get("@odata.nextLink")
        if not next_link:
            break
        url = next_link
    return None

# ====== OneDrive: copiar modelo + link público ======
def copy_template_for_client(client_name: str) -> dict:
    if not TEMPLATE_FILE_PATH or not DEST_FOLDER_ITEM_ID:
        raise RuntimeError("TEMPLATE_FILE_PATH ou DEST_FOLDER_ITEM_ID não definidos.")

    token = get_token()

    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", client_name).strip("_") or "Cliente"
    new_name = f"Planilha_{safe_name}_{stamp}.xlsx"

    copy_url = f"{GRAPH_BASE}{TEMPLATE_FILE_PATH}/copy"
    body = {"parentReference": {"id": DEST_FOLDER_ITEM_ID}, "name": new_name}
    resp = g_post(copy_url, token, data=body)

    if resp.status_code == 202:
        monitor = resp.headers.get("Location")
        if DEBUG:
            print("Copy accepted. Monitor:", monitor)

        total_wait = 0
        max_wait = 300  # até 5min

        if monitor:
            while total_wait < max_wait:
                r2 = requests.get(monitor, headers=g_headers(token))
                if r2.status_code == 202:
                    if DEBUG:
                        print("Still copying... 202")
                    safe_sleep_from_retry_after(r2, default_seconds=3)
                    total_wait += 3
                    continue

                if r2.status_code in (200, 201):
                    try:
                        item = r2.json()
                        if DEBUG:
                            print("Copy finished with payload.")
                        return item
                    except Exception:
                        if DEBUG:
                            print("Copy finished but invalid payload; fallback to listing.")
                        break

                if DEBUG:
                    print("Monitor returned", r2.status_code, "— fallback to listing.")
                break

        if DEBUG:
            print("Fallback: listing children to find", new_name)
        for _ in range(40):  # tenta por ~120s
            item = list_child_by_name(DEST_FOLDER_ITEM_ID, new_name, token)
            if item:
                if DEBUG:
                    print("Found copied item by listing:", item.get("id"))
                return item
            time.sleep(3)

        raise RuntimeError("Timeout ao copiar o modelo (monitor + fallback).")

    # Casos raros de 200/201 com item no body
    try:
        item = resp.json()
        if "id" in item:
            return item
    except Exception:
        pass

    raise RuntimeError(f"Erro inesperado na cópia: {resp.status_code} {resp.text}")

def create_anonymous_link(item_id: str, link_type: str = "edit") -> str:
    token = get_token()
    url = f"{GRAPH_BASE}/drive/items/{item_id}/createLink"
    body = {"type": link_type, "scope": "anonymous"}  # link público
    r = g_post(url, token, data=body)
    data = r.json()
    if "link" not in data or "webUrl" not in data["link"]:
        raise RuntimeError(f"Falha ao criar link público: {r.text}")
    return data["link"]["webUrl"]

# ====== Telegram ======
async def tg_send(chat_id, text):
    async with httpx.AsyncClient(timeout=20) as client:
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text},
        )

# Tarefa em background que faz o trabalho pesado e depois envia a mensagem
async def process_start_async(chat_id: int, first_name: str):
    try:
        await tg_send(chat_id, "🕐 Criando sua planilha. Te aviso quando estiver pronta...")
        # roda a cópia de forma síncrona (IO-bound) numa thread para não travar o event loop
        item = await asyncio.to_thread(copy_template_for_client, first_name)
        item_id = item.get("id")
        if not item_id:
            raise RuntimeError(f"Não consegui obter o ID do arquivo copiado: {json.dumps(item)}")

        # cria link público
        link = await asyncio.to_thread(create_anonymous_link, item_id, "edit")

        await tg_send(
            chat_id,
            "✅ Sua planilha foi criada!\n\n"
            f"📂 Acesse aqui:\n{link}"
        )
    except Exception as e:
        await tg_send(chat_id, f"❌ Erro ao criar a planilha: {e}")

# ====== Rotas ======
@app.get("/")
def root():
    return {"status": "ok"}

@app.post("/telegram/webhook")
async def telegram_webhook(req: Request):
    # IMPORTANTE: responder rápido para não dar timeout/502
    body = await req.json()
    message = body.get("message") or {}
    chat_id = message.get("chat", {}).get("id")
    text = (message.get("text") or "").strip()

    if not chat_id or not text:
        return {"ok": True}

    if text.lower().startswith("/start"):
        first_name = (
            message.get("from", {}).get("first_name")
            or message.get("chat", {}).get("first_name")
            or "Cliente"
        )
        # dispara a tarefa em background e retorna 200 imediatamente
        asyncio.create_task(process_start_async(chat_id, first_name))
        return {"ok": True}

    # resposta rápida para qualquer outra mensagem
    asyncio.create_task(tg_send(chat_id, "Olá! Envie /start para eu criar sua planilha e te mandar o link público."))
    return {"ok": True}
