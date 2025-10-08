import os
import re
import unicodedata
import requests
import httpx
import msal
from datetime import datetime, timedelta
from fastapi import FastAPI, Request

app = FastAPI()

# === CONFIG ===
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

# === MSAL TOKEN ===
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

# === EXCEL APPEND ===
def excel_add_row(values):
    """
    Ordem das colunas:
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

# === UTILS ===
def _strip_accents(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")

MONEY_RE = re.compile(r"(?:r\$\s*)?(\d{1,3}(?:\.\d{3})*(?:,\d{2})?|\d+(?:[.,]\d{2})?)(?!\S)", re.IGNORECASE)

def _find_valor(texto: str):
    m = MONEY_RE.search(texto.replace("R$","R$ "))
    if not m:
        return None
    s = m.group(1).strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return None

def _find_data(texto: str) -> datetime:
    t = _strip_accents(texto.lower())
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

# === 💳 Forma de pagamento aprimorada (com banco + emoji) ===
def _find_forma(texto: str) -> str:
    t = _strip_accents(texto.lower())

    # Pix, dinheiro, boleto
    if "pix" in t:
        return "Pix"
    if "dinheiro" in t:
        return "Dinheiro"
    if "boleto" in t:
        return "Boleto"

    # Cartão + nome (banco/bandeira) após a palavra cartão
    # Exemplos: "cartão santander", "cartão nubank", "cartão visa"
    m = re.search(r"cart[aã]o(?:\s+(?:de\s+)?([\w\-]+))?", t)
    if m:
        nome = m.group(1)
        if nome:
            return f"💳 Cartão {nome.capitalize()}"
        return "💳 Cartão"

    # crédito / débito sem citar "cartão"
    if "credito" in t or "crédito" in t:
        return "💳 Cartão de Crédito"
    if "debito" in t or "débito" in t:
        return "💳 Cartão de Débito"

    return "💳 Cartão"

# === Condição de pgto: agora grava "12x" etc. ===
def _find_condicao(texto: str) -> str:
    t = _strip_accents(texto.lower())

    # 1) "12x" ou "12 x"
    m = re.search(r"\b(\d{1,2})\s*x\b", t)
    if m:
        return f"{int(m.group(1))}x"

    # 2) "12 vezes"
    m = re.search(r"\b(\d{1,2})\s*vezes\b", t)
    if m:
        return f"{int(m.group(1))}x"

    # 3) "parcelado em 12x" / "parcelado em 12 vezes"
    m = re.search(r"parcel\w*\s*(?:em\s*)?(\d{1,2})\s*x", t)
    if m:
        return f"{int(m.group(1))}x"
    m = re.search(r"parcel\w*\s*(?:em\s*)?(\d{1,2})\s*vezes", t)
    if m:
        return f"{int(m.group(1))}x"

    # 4) falou "parcelado" mas sem número → mantém "Parcelado"
    if "parcel" in t:
        return "Parcelado"

    # 5) padrão: À vista
    return "À vista"

# === Grupos e categorias ===
GROUP_FORCE = {
    "🏠": "Gastos Fixos", "gasto fixo": "Gastos Fixos", "fixo": "Gastos Fixos", "fixa": "Gastos Fixos",
    "📺": "Assinatura", "assinatura": "Assinatura", "assinaturas": "Assinatura",
    "💸": "Gastos Variáveis", "variavel": "Gastos Variáveis", "variável": "Gastos Variáveis",
    "🧾": "Despesas Temporárias", "despesa temporaria": "Despesas Temporárias", "temporaria": "Despesas Temporárias",
    "💳": "Pagamento de Fatura", "fatura": "Pagamento de Fatura", "cartao fatura": "Pagamento de Fatura",
    "💵": "Ganhos", "ganho": "Ganhos", "renda": "Ganhos", "salario": "Ganhos", "salário": "Ganhos",
    "💰": "Investimento", "investimento": "Investimento", "investi": "Investimento",
    "📝": "Reserva", "reserva": "Reserva",
}

CATEGORIAS = {
    # Fixos
    "aluguel": ("Aluguel", "Gastos Fixos"),
    "agua": ("Água", "Gastos Fixos"),
    "energia": ("Energia", "Gastos Fixos"),
    "internet": ("Internet", "Gastos Fixos"),
    "plano de saude": ("Plano de Saúde", "Gastos Fixos"),
    "escola": ("Escola", "Gastos Fixos"),

    # Temporárias
    "financiamento": ("Financiamento", "Despesas Temporárias"),
    "iptu": ("IPTU", "Despesas Temporárias"),
    "ipva": ("IPVA", "Despesas Temporárias"),
    "emprestimo": ("Empréstimo", "Despesas Temporárias"),

    # Assinaturas
    "netflix": ("Netflix", "Assinatura"),
    "amazon": ("Amazon", "Assinatura"),
    "disney": ("Disney+", "Assinatura"),
    "premiere": ("Premiere", "Assinatura"),
    "spotify": ("Spotify", "Assinatura"),

    # Variáveis
    "mercado": ("Mercado", "Gastos Variáveis"),
    "supermercado": ("Mercado", "Gastos Variáveis"),
    "farmacia": ("Farmácia", "Gastos Variáveis"),
    "combustivel": ("Combustível", "Gastos Variáveis"),
    "gasolina": ("Gasolina", "Gastos Variáveis"),
    "passeio": ("Passeio em família", "Gastos Variáveis"),
    "ifood": ("iFood", "Gastos Variáveis"),
    "viagem": ("Viagem", "Gastos Variáveis"),

    # Ganhos
    "salario": ("Salário", "Ganhos"),
    "vale": ("Vale", "Ganhos"),
    "renda extra 1": ("Renda Extra 1", "Ganhos"),
    "renda extra 2": ("Renda Extra 2", "Ganhos"),
    "pro labore": ("Pró labore", "Ganhos"),

    # Investimentos
    "renda fixa": ("Renda Fixa", "Investimento"),
    "renda variavel": ("Renda Variável", "Investimento"),
    "fundos imobiliarios": ("Fundos imobiliários", "Investimento"),

    # Reserva
    "trocar de carro": ("Trocar de carro", "Reserva"),
    "viagem pra disney": ("Viagem pra Disney", "Reserva"),
}

FIXO_HINTS = {"mensal", "mensalidade", "assinatura", "plano", "aluguel", "condominio", "luz", "energia", "agua", "internet", "telefone", "iptu", "ipva", "academia", "escola"}

def _force_group_if_asked(texto: str):
    t = _strip_accents(texto.lower())
    for k, g in GROUP_FORCE.items():
        if _strip_accents(k) in t:
            return g
    return None

def _guess_categoria_grupo(texto: str):
    t = _strip_accents(texto.lower())
    for kw, (cat, grp) in CATEGORIAS.items():
        if kw in t:
            return cat, grp, kw
    if any(w in t for w in ["recebi", "ganhei", "entrada", "venda", "salario", "vale"]):
        return "Ganhos", "Ganhos", None
    if any(h in t for h in FIXO_HINTS):
        return "Outros", "Gastos Fixos", None
    return "Outros", "Gastos Variáveis", None

def _find_tipo_por_grupo(grupo: str) -> str:
    return "Entrada" if grupo == "Ganhos" else "Saída"

STOP_PREPS = {"na","no","em","de","do","da","para","pra","por","via","com","sem","ao","a","o","os","as","um","uma","uns","umas"}
VERBOS_GASTO = {"gastei","paguei","comprei","compra","pago","investi","reservei","guardei","pague"}
PAG_PALAVRAS = {"pix","dinheiro","boleto","debito","credito","cartao"}
TEMPO_PALAVRAS = {"hoje","ontem"}

def _extrair_descricao(original: str, kw_cat):
    txt = _strip_accents(original.lower())
    txt = MONEY_RE.sub(" ", txt)
    txt = re.sub(r"\b\d{1,2}[\/\.-]\d{1,2}(?:[\/\.-](\d{2,4}))?\b", " ", txt)
    for w in TEMPO_PALAVRAS | VERBOS_GASTO | PAG_PALAVRAS | STOP_PREPS:
        txt = re.sub(rf"\b{w}\b", " ", txt)
    if kw_cat:
        txt = re.sub(rf"\b{kw_cat}\b", " ", txt)
    for k in GROUP_FORCE.keys():
        txt = re.sub(re.escape(_strip_accents(k)), " ", txt)
    txt = re.sub(r"\b\d{1,2}x\b", " ", txt)
    txt = re.sub(r"[^\w\s]", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    if not txt or (kw_cat and txt == kw_cat):
        return ""
    return txt[:60]

def interpretar_frase(texto: str):
    texto = (texto or "").strip()
    valor = _find_valor(texto)
    if valor is None:
        return None, "Não encontrei o valor na mensagem."
    data_dt = _find_data(texto)
    data_iso = data_dt.strftime("%Y-%m-%d")
    grupo_forcado = _force_group_if_asked(texto)
    categoria, grupo_sugerido, kw_cat = _guess_categoria_grupo(texto)
    grupo = grupo_forcado or grupo_sugerido
    tipo = _find_tipo_por_grupo(grupo)
    forma = _find_forma(texto)
    condicao = _find_condicao(texto)
    descricao = _extrair_descricao(texto, kw_cat)
    row = [data_iso, tipo, grupo, categoria, descricao, float(valor), forma, condicao]
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
        await tg_send(chat_id, "Ex.: 'compra tv 1200 parcelado em 12x no cartão santander' ou '💵 recebi salário 3500'.")
        return {"ok": True}
    try:
        row, err = interpretar_frase(text)
        if err:
            await tg_send(chat_id, f"❗ {err}")
            return {"ok": True}
        if DEBUG:
            await tg_send(chat_id,
                "[DEBUG]\n"
                f"Data: {row[0]}\nTipo: {row[1]}\nGrupo: {row[2]}\nCategoria: {row[3]}\n"
                f"Descrição: {row[4] or '(vazia)'}\nValor: {row[5]:.2f}\nForma: {row[6]}\nCondição: {row[7]}"
            )
        excel_add_row(row)
        await tg_send(chat_id, "✅ Lançado!")
    except Exception as e:
        await tg_send(chat_id, f"❌ Erro: {e}")
    return {"ok": True}
