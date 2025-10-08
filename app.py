import os
from datetime import datetime
from fastapi import FastAPI, Request
import requests
import httpx
import msal

app = FastAPI()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

TEMPLATE_FILE_PATH = os.getenv("TEMPLATE_FILE_PATH")
DEST_FOLDER_ITEM_ID = os.getenv("DEST_FOLDER_ITEM_ID")

CLIENTS_TABLE_PATH = os.getenv("CLIENTS_TABLE_PATH")
CLIENTS_WORKSHEET_NAME = os.getenv("CLIENTS_WORKSHEET_NAME", "Plan1")
CLIENTS_TABLE_NAME = os.getenv("CLIENTS_TABLE_NAME", "Clientes")

DEBUG = os.getenv("DEBUG", "0") == "1"

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
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

def _users_prefix_from_template():
    tfp = (TEMPLATE_FILE_PATH or "").strip()
    if tfp.startswith("/users/"):
        return tfp.split("/drive")[0] + "/drive"
    return "/me/drive"

def normalize_drive_path(raw_path: str) -> str:
    if not raw_path:
        raise RuntimeError("CLIENTS_TABLE_PATH não definido.")
    p = raw_path.strip()
    if p.lower().startswith("http"):
        raise RuntimeError("CLIENTS_TABLE_PATH não deve ser http(s). Use ID ou /users/...")
    if p.startswith("/users/"):
        return p
    if p.startswith("01"):
        upn_drive = _users_prefix_from_template()
        return f"{upn_drive}/items/{p}"
    if p.startswith("/drive/"):
        upn_prefix = _users_prefix_from_template().split("/drive")[0]
        return f"{upn_prefix}{p}"
    upn_drive = _users_prefix_from_template()
    if p.startswith("root:") or p.startswith("/root:") or p.startswith("/drive/root:"):
        suffix = p if p.startswith("/drive/") else f"/{p.lstrip('/')}"
        return f"{upn_drive}{suffix}"
    return f"{upn_drive}/root:/{p.lstrip('/')}"

def create_share_link_anyone_edit(item_id: str):
    token = msal_token()
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

    body = {"type": "edit", "scope": "organization"}
    r2 = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=body, timeout=30)
    if DEBUG:
        print("DEBUG createLink organization status:", r2.status_code, r2.text[:300])
    if r2.status_code < 300:
        try:
            return r2.json()["link"]["webUrl"]
        except Exception:
            pass

    return None

def create_client_copy(chat_id, username):
    token = msal_token()
    upn_drive = _users_prefix_from_template()

    tfp = (TEMPLATE_FILE_PATH or "").strip()
    if "/drive/items/" in tfp or tfp.startswith("/users/"):
        copy_url = f"{GRAPH_BASE}{tfp}/copy" if "/drive/items/" in tfp else f"{GRAPH_BASE}{tfp}:/copy"
    else:
        tfp_norm = normalize_drive_path(tfp)
        copy_url = f"{GRAPH_BASE}{tfp_norm}/copy" if "/drive/items/" in tfp_norm else f"{GRAPH_BASE}{tfp_norm}:/copy"

    safe_user = (username or str(chat_id)).strip().replace(" ", "_")
    copy_name = f"Planilha_{safe_user}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    body = {"parentReference": {"id": DEST_FOLDER_ITEM_ID}, "name": copy_name}

    r = requests.post(
        copy_url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body,
        timeout=30,
    )
    if DEBUG:
        print("DEBUG /copy URL:", copy_url)
        print("DEBUG /copy status:", r.status_code, r.text[:400])
    if r.status_code not in (202, 200, 201):
        raise RuntimeError(f"Erro ao copiar modelo ({r.status_code}): {r.text}")

    location = r.headers.get("Location")
    new_item_id = None
    web_url = None

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
                pass
            else:
                if DEBUG:
                    print("DEBUG polling unexpected:", rr.status_code, rr.text[:400])
                break

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

    share_url = create_share_link_anyone_edit(new_item_id)
    final_url = share_url or web_url or "(sem link)"

    excel_path_items = f"{upn_drive}/items/{new_item_id}"
    register_client(chat_id, username, final_url, excel_path_items)

    return final_url

def register_client(chat_id, username, planilha_url, excel_path_items):
    token = msal_token()
    clients_path = normalize_drive_path(CLIENTS_TABLE_PATH)

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
                "📂 Acesse aqui:\n"
                f"{web_url}"
            )
        except Exception as e:
            await tg_send(chat_id, f"❌ Erro ao criar a planilha: {e}")
        return {"ok": True}

    await tg_send(chat_id, "Envie /start para gerar sua planilha personalizada.")
    return {"ok": True}
