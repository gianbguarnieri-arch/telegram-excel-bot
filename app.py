# app.py
import os
import time
import json
import re
import asyncio
from datetime import datetime

import requests
import msal
from fastapi import FastAPI, Request
import httpx

app = FastAPI()

# ===== ENVs =====
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")

TENANT_ID = os.getenv("TENANT_ID", "")
CLIENT_ID = os.getenv("CLIENT_ID", "")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "")

# Dono do OneDrive onde estão o modelo e a pasta de destino
OWNER_UPN = os.getenv("OWNER_UPN", "").strip()  # ex.: Gian@CecereTecnologia763.onmicrosoft.com

# IDs do OneDrive (não são URLs)
TEMPLATE_ITEM_ID = os.getenv("TEMPLATE_ITEM_ID", "").strip()       # ex.: 01HHVX77QPUSGDU5MVA5C2BW4JKQMBBS2I (Lancamentos.xlsx)
DEST_FOLDER_ITEM_ID = os.getenv("DEST_FOLDER_ITEM_ID", "").strip() # ex.: 01HHVX77TGAHSB7UJHTBBL2JMSQ7DRTSAN (pasta Planilhas)

DEBUG = os.getenv("DEBUG", "0") == "1"

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SCOPE = ["https://graph.microsoft.com/.default"]

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

def hdr(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def g_post(url: str, token: str, data: dict | None = None):
    if DEBUG:
        print("POST", url, "DATA", data)
    r = requests.post(url, headers=hdr(token), json=data)
    if r.status_code >= 300 and r.status_code != 202:
        raise RuntimeError(f"Graph POST {r.status_code}: {r.text}")
    return r

def g_get(url: str, token: str):
    if DEBUG:
        print("GET", url)
    r = requests.get(url, headers=hdr(token))
    if r.status_code >= 300:
        raise RuntimeError(f"Graph GET {r.status_code}: {r.text}")
    return r

def safe_sleep_from_retry_after(resp, default_seconds=2):
    ra = resp.headers.get("Retry-After")
    try:
        wait = int(ra) if ra is not None else default_seconds
    except ValueError:
        wait = default_seconds
    time.sleep(max(wait, 1))

def list_child_by_name(owner_upn: str, dest_folder_id: str, name: str, token: str):
    url = f"{GRAPH_BASE}/users/{owner_upn}/drive/items/{dest_folder_id}/children?$select=id,name,lastModifiedDateTime&$top=200"
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

def copy_template_for_client(client_name: str) -> dict:
    """
    Copia /users/{OWNER_UPN}/drive/items/{TEMPLATE_ITEM_ID} para a pasta DEST_FOLDER_ITEM_ID,
    com nome "Planilha_{client}_{stamp}.xlsx". Faz polling e fallback por listagem.
    """
    if not (OWNER_UPN and TEMPLATE_ITEM_ID and DEST_FOLDER_ITEM_ID):
        raise RuntimeError("OWNER_UPN, TEMPLATE_ITEM_ID ou DEST_FOLDER_ITEM_ID não definidos.")

    token = get_token()

    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", client_name).strip("_") or "Cliente"
    new_name = f"Planilha_{safe_name}_{stamp}.xlsx"

    copy_url = f"{GRAPH_BASE}/users/{OWNER_UPN}/drive/items/{TEMPLATE_ITEM_ID}/copy"
    body = {"parentReference": {"id": DEST_FOLDER_ITEM_ID}, "name": new_name}
    resp = g_post(copy_url, token, data=body)

    if resp.status_code == 202:
        monitor = resp.headers.get("Location")
        if DEBUG:
            print("Copy accepted. Monitor:", monitor)

        total_wait = 0
        max_wait = 300  # 5 min

        if monitor:
            while total_wait < max_wait:
                r2 = requests.get(monitor, headers=hdr(token))
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

        # Fallback: procurar pelo nome na pasta
        if DEBUG:
            print("Fallback: listing children to find", new_name)
        for _ in range(40):  # ~120s
            item = list_child_by_name(OWNER_UPN, DEST_FOLDER_ITEM_ID, new_name, token)
            if item:
                if DEBUG:
                    print("Found copied item by listing:", item.get("id"))
                return item
            time.sleep(3)

        raise RuntimeError("Timeout ao copiar o modelo (monitor + fallback).")

    # Casos raros 200/201 com item no body
    try:
        item = resp.json()
        if "id" in item:
            return item
    except Exception:
        pass

    raise RuntimeError(f"Erro inesperado na cópia: {resp.status_code} {resp.text}")

def create_anonymous_link(owner_upn: str, item_id: str, link_type: str = "edit") -> str:
    token = get_token()
    url = f"{GRAPH_BASE}/users/{owner_upn}/drive/items/{item_id}/createLink"
    body = {"type": link_type, "scope": "anonymous"}
    r = g_post(url, token, data=body)
    data = r.json()
    if "link" not in data or "webUrl" not in data["link"]:
        raise RuntimeError(f"Falha ao criar link público: {r.text}")
    return data["link"]["webUrl"]

async def tg_send(chat_id, text):
    async with httpx.AsyncClient(timeout=20) as client:
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text},
        )

async def process_start_async(chat_id: int, first_name: str):
    try:
        await tg_send(chat_id, "🕐 Criando sua planilha. Te aviso quando estiver pronta...")
        item = await asyncio.to_thread(copy_template_for_client, first_name)
        item_id = item.get("id")
        if not item_id:
            raise RuntimeError(f"Não consegui obter o ID do arquivo copiado: {json.dumps(item)}")
        link = await asyncio.to_thread(create_anonymous_link, OWNER_UPN, item_id, "edit")
        await tg_send(
            chat_id,
            "✅ Sua planilha foi criada!\n\n"
            f"📂 Acesse aqui:\n{link}"
        )
    except Exception as e:
        await tg_send(chat_id, f"❌ Erro ao criar a planilha: {e}")

@app.get("/")
def root():
    return {"status": "ok"}

@app.post("/telegram/webhook")
async def telegram_webhook(req: Request):
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
        asyncio.create_task(process_start_async(chat_id, first_name))
        return {"ok": True}

    asyncio.create_task(tg_send(chat_id, "Olá! Envie /start para eu criar sua planilha e te mandar o link público."))
    return {"ok": True}
