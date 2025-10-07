import os
import re
from datetime import datetime

from fastapi import FastAPI, Request
import requests
import httpx
import msal

app = FastAPI()

# ===== Telegram =====
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

# ===== Graph / Excel =====
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Ex.: /me/drive/root:/Planilhas/Lancamentos.xlsx
EXCEL_PATH = os.getenv("EXCEL_PATH", "/me/drive/root:/Planilhas/Lancamentos.xlsx")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Plan1")
TABLE_NAME = os.getenv("TABLE_NAME", "Lancamentos")
SCOPE = ["https://graph.microsoft.com/.default"]

# ===== Helpers =====
def msal_token():
    """Obtem token de app (client credentials) para chamar o Microsoft Graph."""
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

def excel_add_row(values):
    """
    Adiciona uma linha na Tabela do Excel.
    values deve ser uma lista na ordem exata dos cabeçalhos:
    [DataISO, Tipo, Categoria, Descricao, ValorFloat, FormaPagamento, Origem]
    """
    token = msal_token()
    url = (
        f"{GRAPH_BASE}{EXCEL_PATH}:/workbook/worksheets('{WORKSHEET_NAME}')"
        f"/tables('{TABLE_NAME}')/rows/add"
    )
    payload = {"values": [values]}
    r = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=payload, timeout=20)
    if r.status_code >= 300:
        raise RuntimeError(f"Graph error {r.status_code}: {r.text}")
    return r.json()

ADD_REGEX = re.compile(r"^/add\s+(.+)$", re.IGNORECASE)

def parse_add(text):
    """
    Espera: /add DD/MM/AAAA;Tipo;Categoria;Descricao;Valor;FormaPagamento
    Retorna: [DataISO, Tipo, Categoria, Descricao, ValorFloat, FormaPagamento, Origem]
    """
    m = ADD_REGEX.match(text.strip())
    if not m:
        return None, "Formato: /add DD/MM/AAAA;Tipo;Categoria;Descricao;Valor;FormaPagamento"
    parts = [p.strip() for p in m.group(1).split(";")]
    if len(parts) != 6:
        return None, "Faltam campos. Use 6 campos separados por ;"

    data_br, tipo, categoria, descricao, valor_str, forma = parts

    # Data → ISO
    try:
        dt = datetime.strptime(data_br, "%d/%m/%Y")
        data_iso = dt.strftime("%Y-%m-%d")
    except Exception:
        return None, "Data inválida. Use DD/MM/AAAA."

    # Valor (troca vírgula por ponto)
    valor_clean = valor_str.replace(".", "").replace(",", ".")
    try:
        valor = float(valor_clean)
    except Exception:
        return None, "Valor inválido. Ex.: 123,45"

    origem = "Telegram"
    return [data_iso, tipo, categoria, descricao, valor, forma, origem], None

async def tg_send(chat_id, text):
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text},
        )

# ===== Routes =====
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
        reply = (
            "Olá! Para adicionar um lançamento, envie:\n"
            "/add DD/MM/AAAA;Tipo;Categoria;Descricao;Valor;FormaPagamento\n"
            "Ex.: /add 06/10/2025;Compra;Mercado;Almoço;45,90;Cartão"
        )
        await tg_send(chat_id, reply)
        return {"ok": True}

    # /add ...
    row, err = parse_add(text)
    if err:
        await tg_send(chat_id, f"❗ {err}")
        return {"ok": True}

    try:
        excel_add_row(row)
        await tg_send(chat_id, "✅ Lançamento adicionado com sucesso!")
    except Exception as e:
        # Manda o erro bruto pro chat pra facilitar debug inicial
        await tg_send(chat_id, f"❌ Erro ao lançar no Excel: {e}")

    return {"ok": True}
