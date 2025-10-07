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
EXCEL_PATH = (os.getenv("EXCEL_PATH") or "").strip()
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
    """
    values deve ter EXATAMENTE 8 posições na ordem:
    [Data, Tipo, Grupo, Categoria, Descrição, Valor, Forma de pgto, Condição de pgto]
    """
    token = msal_token()
    if "/drive/items/" in EXCEL_PATH:
        url = f"{GRAPH_BASE}{EXCEL_PATH}/workbook/worksheets('{WORKSHEET_NAME}')/tables('{TABLE_NAME}')/rows/add"
    else:
        url = f"{GRAPH_BASE}{EXCEL_PATH}:/workbook/worksheets('{WORKSHEET_NAME}')/tables('{TABLE_NAME}')/rows/add"

    payload = {"values": [values]}
    r = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=payload, timeout=25)
    if r.status_code >= 300:
        raise RuntimeError(f"Graph error {r.status_code}: {r.text}")
    return r.json()

# === PARSER (linguagem natural) ===

MONEY_RE = re.compile(r"(?:r\$\s*)?(\d{1,3}(?:\.\d{3})*,\d{2}|\d+[,\.]\d{2})", re.IGNORECASE)
DATE_ANY_RE = re.compile(r"(\b\d{1,2}[\/\.-]\d{1,2}(?:[\/\.-]\d{2,4})?\b|\bhoje\b|\bontem\b)", re.IGNORECASE)

CATEGORIAS = {
    "mercado": ("Alimentação", "Despesas Variáveis"),
    "supermercado": ("Alimentação", "Despesas Variáveis"),
    "padaria": ("Alimentação", "Despesas Variáveis"),
    "restaurante": ("Alimentação", "Despesas Variáveis"),
    "almoco": ("Alimentação", "Despesas Variáveis"),
    "almoço": ("Alimentação", "Despesas Variáveis"),
    "ifood": ("Alimentação", "Despesas Variáveis"),

    "aluguel": ("Moradia", "Despesas Fixas"),
    "condominio": ("Moradia", "Despesas Fixas"),
    "condomínio": ("Moradia", "Despesas Fixas"),
    "luz": ("Contas", "Despesas Fixas"),
    "energia": ("Contas", "Despesas Fixas"),
    "agua": ("Contas", "Despesas Fixas"),
    "água": ("Contas", "Despesas Fixas"),
    "internet": ("Contas", "Despesas Fixas"),
    "telefone": ("Contas", "Despesas Fixas"),

    "gasolina": ("Transporte", "Despesas Variáveis"),
    "combustivel": ("Transporte", "Despesas Variáveis"),
    "combustível": ("Transporte", "Despesas Variáveis"),
    "uber": ("Transporte", "Despesas Variáveis"),
    "taxi": ("Transporte", "Despesas Variáveis"),
    "táxi": ("Transporte", "Despesas Variáveis"),

    "farmacia": ("Saúde", "Despesas Variáveis"),
    "farmácia": ("Saúde", "Despesas Variáveis"),

    "netflix": ("Entretenimento", "Despesas Variáveis"),
    "spotify": ("Entretenimento", "Despesas Variáveis"),
}

def _find_valor(texto: str):
    m = MONEY_RE.search(texto)
    if not m:
        return None
    s = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return None

def _find_data(texto: str) -> datetime:
    t = texto.lower()
    hoje = datetime.now()
    if "ontem" in t:
        return hoje - timedelta(days=1)
    if "hoje" in t:
        return hoje
    m = re.search(r"(\d{1,2})[\/\.-](\d{1,2})(?:[\/\.-](\d{2,4}))?", t)
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3)
        if not y:
            y = str(hoje.year)
        elif len(y) == 2:
            y = f"20{y}"
        try:
            return datetime.strptime(f"{int(d):02d}/{int(mo):02d}/{int(y):04d}", "%d/%m/%Y")
        except:
            pass
    return hoje

def _find_forma(texto: str) -> str:
    t = texto.lower()
    if "pix" in t: return "Pix"
    if "dinheiro" in t: return "Dinheiro"
    if "boleto" in t: return "Boleto"
    if "debito" in t or "débito" in t: return "Cartão"
    if "credito" in t or "crédito" in t or "cartao" in t or "cartão" in t: return "Cartão"
    return "Cartão"

def _find_condicao(texto: str) -> str:
    t = texto.lower().replace(" ", "")
    # 2x, 3x, ... ou palavras com "parcel"
    if re.search(r"\b(\d{1,2})x\b", t) or "parcel" in t:
        return "Parcelado"
    return "À vista"

def _find_tipo(texto: str) -> str:
    t = texto.lower()
    if any(w in t for w in ["recebi", "entrada", "venda", "ganhei"]):
        return "Receita"
    if any(w in t for w in ["gastei", "paguei", "compra", "pago"]):
        return "Compra"
    return "Movimento"

def _find_categoria_grupo(texto: str):
    t = texto.lower()
    for kw, (cat, grp) in CATEGORIAS.items():
        if kw in t:
            return cat, grp
    # fallback
    tipo = _find_tipo(texto)
    if tipo == "Receita":
        return "Receitas", "Receitas"
    # padrão despesas variáveis
    return "Outros", "Despesas Variáveis"

def interpretar_frase(texto: str):
    texto = (texto or "").strip()
    valor = _find_valor(texto)
    if valor is None:
        return None, "Não encontrei o valor na mensagem."

    data_dt = _find_data(texto)
    data_iso = data_dt.strftime("%Y-%m-%d")

    tipo = _find_tipo(texto)
    categoria, grupo = _find_categoria_grupo(texto)
    forma = _find_forma(texto)
    condicao = _find_condicao(texto)

    descricao = texto  # guarda frase original

    # ORDEM das 8 colunas:
    row = [
        data_iso,        # Data
        tipo,            # Tipo
        grupo,           # Grupo
        categoria,       # Categoria
        descricao,       # Descrição
        valor,           # Valor (float)
        forma,           # Forma de pgto
        condicao,        # Condição de pgto
    ]
    return row, None

# === TELEGRAM ===
async def tg_send(chat_id, text):
    async with httpx.AsyncClient(timeout=12) as client:
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
        await tg_send(chat_id, "Mande algo como: 'gastei 45,90 no mercado com cartão hoje 2x'")
        return {"ok": True}

    try:
        row, err = interpretar_frase(text)
        if err:
            await tg_send(chat_id, f"❗ {err}")
            return {"ok": True}

        if DEBUG:
            await tg_send(chat_id,
                "[DEBUG]\n"
                f"Data: {row[0]}\nTipo: {row[1]}\nGrupo: {row[2]}\n"
                f"Categoria: {row[3]}\nValor: {row[5]:.2f}\n"
                f"Forma: {row[6]}\nCondição: {row[7]}\nDesc: {row[4]}"
            )

        excel_add_row(row)
        await tg_send(chat_id, "✅ Lançado!")
    except Exception as e:
        await tg_send(chat_id, f"❌ Erro: {e}")

    return {"ok": True}
