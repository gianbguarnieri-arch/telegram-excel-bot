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
# OneDrive/SharePoint: copiar modelo e criar link público
# ----------------------------
def copy_template_for_client(client_name: str) -> dict:
    """
    Copia o arquivo modelo para a pasta DEST_FOLDER_ITEM_ID com um nome
    amigável e retorna o JSON do item recém-criado (inclui 'id').
    """
    if not TEMPLATE_FILE_PATH or not DEST_FOLDER_ITEM_ID:
        raise RuntimeError("TEMPLATE_FILE_PATH ou DEST_FOLDER_ITEM_ID não definidos.")

    token = get_token()

    # nome final do arquivo
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", client_name).strip("_")
    new_name = f"Planilha_{safe_name}_{stamp}.xlsx"

    # POST /drive/items/{item-id}/copy
    # Observação: TEMPLATE_FILE_PATH está no formato /users/.../drive/items/{ID}
    copy_url = f"{GRAPH_BASE}{TEMPLATE_FILE_PATH}/copy"
    body = {
        "parentReference": {"id": DEST_FOLDER_ITEM_ID},
        "name": new_name,
    }
    resp = g_post(copy_url, token, data=body)

    if resp.status_code not in (202,):  # Accepted
        raise RuntimeError(f"Erro ao copiar modelo: {resp.text}")

    # Polling do monitor até concluir
    monitor = resp.headers.get("Location")
    if not monitor:
        # fallback: às vezes a API retorna de imediato o item no body (raro)
        try:
            return resp.json()
        except Exception:
            raise RuntimeError("Copia iniciada, mas sem Location para monitorar.")

    # Aguarda concluir
    for _ in range(40):  # ~40 * 1s = 40s
        time.sleep(1)
        r2 = requests.get(monitor, headers=g_headers(token))
        if r2.status_code == 202:
            # ainda processando
            continue
        if r2.status_code in (200, 201):
            try:
                item = r2.json()
                return item
            except Exception:
                break
        # qualquer outra situação
        break

    raise RuntimeError("Timeout ao copiar o modelo (monitor).")

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

    # /start -> cria a planilha do cliente e envia link público
    if text.lower().startswith("/start"):
        # Tenta usar o primeiro nome como "nome do cliente" se existir
        first_name = message.get("from", {}).get("first_name") or "Cliente"
        try:
            # 1) Copia o modelo para a pasta destino
            new_item = copy_template_for_client(first_name)
            new_id = new_item.get("id")
            if not new_id:
                raise RuntimeError(f"Não obtive ID do novo arquivo: {json.dumps(new_item)}")

            # 2) Cria link público (qualquer pessoa com o link)
            #    Use 'edit' se quiser permitir edição
            public_link = create_anonymous_link(new_id, link_type="edit")

            await tg_send(
                chat_id,
                "✅ Sua planilha foi criada!\n\n"
                f"📂 Acesse aqui:\n{public_link}"
            )
        except Exception as e:
            await tg_send(chat_id, f"❌ Erro ao criar a planilha: {e}")
        return {"ok": True}

    # mensagem padrão
    await tg_send(
        chat_id,
        "Olá! Envie /start para eu criar sua planilha e te mandar o link público."
    )
    return {"ok": True}
