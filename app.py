import os
import re
import requests
import httpx
import msal
from datetime import datetime, timedelta
from fastapi import FastAPI, Request

app = FastAPI()

# === CONFIGURAÇÕES ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
EXCEL_PATH = os.getenv("EXCEL_PATH")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Plan1")
TABLE_NAME = os.getenv("TABLE_NAME", "Lancamentos")
SCOPE = ["https://graph.microsoft.com/.default"]
DEBUG = os.getenv("DEBUG", "0") == "1"

# === TOKEN MICROSOFT ===
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

# === ENVIO PARA EXCEL ===
def excel_add_row(values):
    token = msal_token()
    if "/drive/items/" in EXCEL_PATH:
        url = f"{GRAPH_BASE}{EXCEL_PATH}/workbook/worksheets('{WORKSHEET_NAME}')/tables('{TABLE_NAME}')/rows/add"
    else:
        url = f"{GRAPH_BASE}{EXCEL_PATH}:/workbook/worksheets('{WORKSHEET_NAME}')/tables('{TABLE_NAME}')/rows/add"

    payload = {"values": [values]}
    r = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=payload)
    if r.status_code >= 300:
        raise RuntimeError(f"Graph error {r.status_code}: {r.text}")
    return r.json()

# === INTERPRETAÇÃO NATURAL ===
def interpretar_frase(texto):
    texto = texto.lower().strip()

    # valor
    valor = None
    match_valor = re.search(r"(\d+[,\.]?\d{0,2})", texto)
    if match_valor:
        valor = float(match_valor.group(1).replace(",", "."))
    else:
        return None, "Não encontrei o valor na mensagem."

    # forma de pagamento
    if "pix" in texto:
        forma = "Pix"
    elif "cartão" in texto or "credito" in texto or "débito" in texto:
        forma = "Cartão"
    elif "dinheiro" in texto:
        forma = "Dinheiro"
    else:
        forma = "Outros"

    # data
    hoje = datetime.now()
    if "ontem" in texto:
        data = hoje - timedelta(days=1)
    elif "hoje" in texto:
        data = hoje
    else:
        match_data = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", texto)
        if match_data:
            try:
                data = datetime.strptime(match_data.group(1), "%d/%m/%Y")
            except:
                return None, "Data inválida."
        else:
            data = hoje

    data_iso = data.strftime("%Y-%m-%d")

    # tipo e categoria
    tipo = "Compra" if "gastei" in texto or "paguei" in texto else "Receita" if "recebi" in texto else "Movimento"

    # categorias simples
    categorias = {
        "mercado": "Alimentação",
        "supermercado": "Alimentação",
        "gasolina": "Transporte",
        "uber": "Transporte",
        "ifood": "Alimentação",
        "almoço": "Alimentação",
        "aluguel": "Moradia",
        "luz": "Contas",
        "energia": "Contas",
        "água": "Contas",
        "netflix": "Entretenimento",
        "spotify": "Entretenimento",
    }
    categoria = "Outros"
    for palavra, cat in categorias.items():
        if palavra in texto:
            categoria = cat
            break

    descricao = texto
    origem = "Telegram"

    return [data_iso, tipo, categoria, descricao, valor, forma, origem], None

# === TELEGRAM ===
async def tg_send(chat_id, text):
    async with httpx.AsyncClient() as client:
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
    msg = body.get("message", {})
    chat_id = msg.get("chat", {}).get("id")
    text = (msg.get("text") or "").strip()

    if not chat_id or not text:
        return {"ok": True}

    if text.lower().startswith("/start"):
        await tg_send(chat_id, "Olá! Me diga algo como: 'gastei 45,90 no mercado com cartão hoje'")
        return {"ok": True}

    try:
        dados, erro = interpretar_frase(text)
        if erro:
            await tg_send(chat_id, f"❗ {erro}")
            if DEBUG:
                print(f"[DEBUG] Erro de parsing: {erro}")
            return {"ok": True}

        excel_add_row(dados)
        await tg_send(chat_id, "✅ Lançado!")
    except Exception as e:
        msg_erro = f"❌ Erro: {e}"
        await tg_send(chat_id, msg_erro)
        if DEBUG:
            print(f"[DEBUG] {msg_erro}")

    return {"ok": True"}
