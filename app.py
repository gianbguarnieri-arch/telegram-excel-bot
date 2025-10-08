# app.py
import os
import time
import json
import re
from datetime import datetime

import requests
import msal
from fastapi import FastAPI, Request
import httpx

# ----------------------------
# Config
# ----------------------------
app = FastAPI()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TENANT_ID = os.getenv("TENANT_ID", "")
CLIENT_ID = os.getenv("CLIENT_ID", "")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "")

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Modelo a ser copiado e pasta destino
TEMPLATE_FILE_PATH = os.getenv("TEMPLATE_FILE_PATH", "").strip()
DEST_FOLDER_ITEM_ID = os.getenv("DEST_FOLDER_ITEM_ID", "").strip()

DEBUG = os.getenv("DEBUG", "0") == "1"

SCOPE = ["https://graph.microsoft.com/.default"]

# ----------------------------
# MSAL / Graph helpers
# ----------------------------
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

# ----------------------------
# Utils
# ----------------------------
def safe_sleep_from_retry_after(resp, default_seconds=2):
    ra = resp.headers.get("Retry-After")
    try:
        wait = int(ra) if ra is not None else default_seconds
    except ValueError:
        wait = default_seconds
    time.sleep(max(wait, 1))

def list_child_by_name(dest_folder_id: str, name: str, token: str):
    """
    Procura por um item com determinado 'name' dentro de uma pasta (por ID).
    Retorna o JSON do item se achar, senão None.
    """
    url = (
        f"{GRAPH_BASE}/drive/items/{dest_folder_id}/children"
        f"?$select=id,name,lastModifiedDateTime&$top=200"
    )
    while True:
        r = g_get(url, token)
        data = r.json()
        for it in data.get("value", []):
            if it.get("name") == name:
                return it
        # paginação
        next_link = data.get("@odata.nextLink")
        if not next_link:
            break
        url = next_link
    return None

# ----------------------------
# OneDrive/SharePoint: copiar modelo e criar link público
# ----------------------------
def copy_template_for_client(client_name: str) -> dict:
    """
    Copia o arquivo modelo para a pasta DEST_FOLDER_ITEM_ID com um nome
    amigável e retorna o JSON do item recém-criado (inclui 'id').
    Também implementa polling robusto + fallback por listagem.
    """
    if not TEMPLATE_FILE_PATH or not DEST_FOLDER_ITEM_ID:
        raise RuntimeError("TEMPLATE_FILE_PATH ou DEST_FOLDER_ITEM_ID não definidos.")

    token = get_token()

    # nome final do arquivo
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", client_name).strip("_")
    new_name = f"Planilha_{safe_name}_{stamp}.xlsx"

    # POST /drive/items/{item-id}/copy
    copy_url = f"{GRAPH_BASE}{TEMPLATE_FILE_PATH}/copy"
    body = {
        "parentReference": {"id": DEST_FOLDER_ITEM_ID},
        "name": new_name,
    }
    resp = g_post(copy_url, token, data=body)

    # A API costuma devolver 202 + Location para monitorar
    if resp.status_code == 202:
        monitor = resp.headers.get("Location")
        if DEBUG:
            print("Copy accepted. Monitor:", monitor)

        # Polling por até ~5 minutos
        total_wait = 0
        max_wait = 300  # 5min

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
                    # Concluiu com payload do item
                    try:
                        item = r2.json()
                        if DEBUG:
                            print("Copy finished with payload.")
                        return item
                    except Exception:
                        # Se der erro para json, tenta fallback
                        if DEBUG:
                            print("Copy finished but invalid payload; fallback to listing.")
                        break

                # Qualquer outro status: tenta fallback
                if DEBUG:
                    print("Monitor returned", r2.status_code, "— fallback to listing.")
                break

        # Fallback: procurar pelo nome na pasta de destino
        if DEBUG:
            print("Fallback: listing children to find", new_name)
        for _ in range(40):  # tenta por ~40*3=120s, caso a visibilidade demore
            item = list_child_by_name(DEST_FOLDER_ITEM_ID, new_name, token)
            if item:
                if DEBUG:
                    print("Found copied item by listing:", item.get("id"))
                return item
            time.sleep(3)

        raise RuntimeError("Timeout ao copiar o modelo (monitor + fallback).")

    # Em alguns casos raros a API devolve 200/201 com o item direto
    try:
        item = resp.json()
        if "id" in item:
            return item
    except Exception:
        pass

    raise RuntimeError(f"Erro inesperado na cópia: {resp.status_code} {resp.text}")

def create_anonymous_link(item_id: str, link_type: str = "view") -> str:
    """
    Cria um link público (anonymous) para o item (arquivo) indicado.
    link_type: 'view' ou 'edit'
    Retorna a URL web pública.
    """
    token = get_token()
    url = f"{GRAPH_BASE}/drive/items/{item_id}/createLink"
    body = {
        "type": link_type,       # 'view' para leitura, 'edit' para edição
        "scope": "anonymous"     # <-- parte crucial para 'qualquer pessoa com o link'
    }
    r = g_post(url, token, data=body)
    data = r.json()
    if "link" not in data or "webUrl" not in data["link"]:
        raise RuntimeError(f"Falha ao criar link público: {r.text}")
    return data["link"]["webUrl"]

# ----------------------------
# Telegram helpers
# ----------------------------
async def tg_send(chat_id: int | str, text: str):
    async with httpx.AsyncClient(timeout=15) as client:
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text},
        )

# ----------------------------
# Rotas
# ----------------------------
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
        first_name = message.get("from", {}).get("first_name") or "Cliente"
        try:
            new_item = copy_template_for_client(first_name)
            new_id = new_item.get("id")
            if not new_id:
                raise RuntimeError(f"Não obtive ID do novo arquivo: {json.dumps(new_item)}")

            public_link = create_anonymous_link(new_id, link_type="edit")  # mude para 'view' se quiser só leitura

            await tg_send(
                chat_id,
                "✅ Sua planilha foi criada!\n\n"
                f"📂 Acesse aqui:\n{public_link}"
            )
        except Exception as e:
            await tg_send(chat_id, f"❌ Erro ao criar a planilha: {e}")
        return {"ok": True}

    await tg_send(
        chat_id,
        "Olá! Envie /start para eu criar sua planilha e te mandar o link público."
    )
    return {"ok": True}
