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
    Ordem das colunas na tabela:
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

# valores aceitos: "R$ 1.234,56" | "1234,56" | "1234.56" | "34"
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

def _find_forma(texto: str) -> str:
    t = _strip_accents(texto.lower())
    if "pix" in t: return "Pix"
    if "dinheiro" in t: return "Dinheiro"
    if "boleto" in t: return "Boleto"
    if "debito" in t or "débito" in t: return "Cartão"
    if "credito" in t or "crédito" in t or "cartao" in t or "cartão" in t: return "Cartão"
    return "Cartão"

def _find_condicao(texto: str) -> str:
    t = _strip_accents(texto.lower()).replace(" ", "")
    if re.search(r"\b(\d{1,2})x\b", t) or "parcel" in t:
        return "Parcelado"
    return "À vista"

# === GRUPOS & CATEGORIAS ===
# Palavras/emoji para forçar grupo
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

# Mapeamento de categorias → grupo padrão
# (o usuário pode forçar o grupo com palavras/emoji acima)
CATEGORIAS = {
    # 🏠 Gastos Fixos (exemplos)
    "aluguel": ("Aluguel", "Gastos Fixos"),
    "agua": ("Água", "Gastos Fixos"),
    "água": ("Água", "Gastos Fixos"),
    "energia": ("Energia", "Gastos Fixos"),
    "luz": ("Energia", "Gastos Fixos"),
    "internet": ("Internet", "Gastos Fixos"),
    "plano de saude": ("Plano de Saúde", "Gastos Fixos"),
    "plano de saúde": ("Plano de Saúde", "Gastos Fixos"),
    "escola": ("Escola", "Gastos Fixos"),

    # 🧾 Despesas Temporárias (exemplos)
    "financiamento": ("Financiamento", "Despesas Temporárias"),
    "iptu": ("IPTU", "Despesas Temporárias"),
    "ipva": ("IPVA", "Despesas Temporárias"),
    "emprestimo": ("Empréstimo", "Despesas Temporárias"),
    "empréstimo": ("Empréstimo", "Despesas Temporárias"),

    # 📺 Assinatura (exemplos)
    "netflix": ("Netflix", "Assinatura"),
    "amazon": ("Amazon", "Assinatura"),
    "disney": ("Disney+", "Assinatura"),
    "disney+": ("Disney+", "Assinatura"),
    "premiere": ("Premiere", "Assinatura"),
    "spotify": ("Spotify", "Assinatura"),

    # 💸 Gastos Variáveis (exemplos)
    "mercado": ("Mercado", "Gastos Variáveis"),
    "supermercado": ("Mercado", "Gastos Variáveis"),
    "farmacia": ("Farmácia", "Gastos Variáveis"),
    "farmácia": ("Farmácia", "Gastos Variáveis"),
    "combustivel": ("Combustível", "Gastos Variáveis"),
    "combustível": ("Combustível", "Gastos Variáveis"),
    "gasolina": ("Gasolina", "Gastos Variáveis"),
    "passeio": ("Passeio em família", "Gastos Variáveis"),
    "passeio em familia": ("Passeio em família", "Gastos Variáveis"),
    "ifood": ("iFood", "Gastos Variáveis"),
    "viagem": ("Viagem", "Gastos Variáveis"),

    # 💵 Ganhos (exemplos)
    "salario": ("Salário", "Ganhos"),
    "salário": ("Salário", "Ganhos"),
    "vale": ("Vale", "Ganhos"),
    "renda extra 1": ("Renda Extra 1", "Ganhos"),
    "renda extra 2": ("Renda Extra 2", "Ganhos"),
    "pro labore": ("Pró labore", "Ganhos"),
    "pró labore": ("Pró labore", "Ganhos"),

    # 💰 Investimento (exemplos)
    "renda fixa": ("Renda Fixa", "Investimento"),
    "renda variavel": ("Renda Variável", "Investimento"),
    "renda variável": ("Renda Variável", "Investimento"),
    "fundos imobiliarios": ("Fundos imobiliários", "Investimento"),
    "fundos imobiliários": ("Fundos imobiliários", "Investimento"),

    # 📝 Reserva (exemplos)
    "trocar de carro": ("Trocar de carro", "Reserva"),
    "viagem pra disney": ("Viagem pra Disney", "Reserva"),
}

# Palavras que indicam gasto recorrente/conta (ajudam a classificar quando não bater um exemplo)
FIXO_HINTS = {
    "mensal", "mensalidade", "assinatura", "plano",
    "aluguel", "condominio", "condomínio", "luz", "energia", "agua", "água",
    "internet", "telefone", "iptu", "ipva", "academia", "escola",
}

def _force_group_if_asked(texto: str) -> str | None:
    t = _strip_accents(texto.lower())
    for k, g in GROUP_FORCE.items():
        if _strip_accents(k) in t:
            return g
    return None

def _guess_categoria_grupo(texto: str):
    t = _strip_accents(texto.lower())
    # 1) tentamos mapear pelas categorias conhecidas
    for kw, (cat, grp) in CATEGORIAS.items():
        if kw in t:
            return cat, grp, kw
    # 2) se tiver “ganhei/recebi/entrada/venda” → grupo Ganhos
    if any(w in t for w in ["recebi", "ganhei", "entrada", "venda", "salario", "salário", "vale"]):
        return "Ganhos", "Ganhos", None
    # 3) se houver dica de fixo
    if any(h in t for h in FIXO_HINTS):
        return "Outros", "Gastos Fixos", None
    # 4) fallback variável
    return "Outros", "Gastos Variáveis", None

def _find_tipo_por_grupo(grupo: str) -> str:
    if grupo == "Ganhos":
        return "Entrada"
    # Para Investimento e Reserva estamos tratando como saída (dinheiro sai da conta)
    return "Saída"

# Descrição opcional: remove valor, datas, verbos, preposições, palavras de pagamento e a palavra da categoria
STOP_PREPS = {
    "na","no","em","de","do","da","para","pra","por","via","com","sem",
    "ao","a","o","os","as","um","uma","uns","umas"
}
VERBOS_GASTO = {"gastei","paguei","comprei","compra","pago","investi","destinei","reservei","guardei","pague"}
PAG_PALAVRAS = {"pix","dinheiro","boleto","debito","débito","credito","crédito","cartao","cartão"}
TEMPO_PALAVRAS = {"hoje","ontem"}

def _extrair_descricao(original: str, kw_cat: str | None) -> str:
    txt = _strip_accents(original.lower())
    # remove valor
    txt = MONEY_RE.sub(" ", txt)
    # remove datas/tempo
    txt = re.sub(r"\b\d{1,2}[\/\.-]\d{1,2}(?:[\/\.-](\d{2,4}))?\b", " ", txt)
    for w in TEMPO_PALAVRAS | VERBOS_GASTO | PAG_PALAVRAS | STOP_PREPS:
        txt = re.sub(rf"\b{w}\b", " ", txt)
    if kw_cat:
        txt = re.sub(rf"\b{kw_cat}\b", " ", txt)
    # força/emoji de grupo também não entra na descrição
    for k in GROUP_FORCE.keys():
        txt = re.sub(re.escape(_strip_accents(k)), " ", txt)
    # parcelas 2x/3x
    txt = re.sub(r"\b\d{1,2}x\b", " ", txt)
    # normaliza
    txt = re.sub(r"[^\w\s]", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    # evita vazio / redundância
    if not txt:
        return ""
    if kw_cat and txt == kw_cat:
        return ""
    return txt[:60]

def interpretar_frase(texto: str):
    texto = (texto or "").strip()
    valor = _find_valor(texto)
    if valor is None:
        return None, "Não encontrei o valor na mensagem."

    data_dt = _find_data(texto)
    data_iso = data_dt.strftime("%Y-%m-%d")

    # 1) Grupo forçado pelo usuário?
    grupo_forcado = _force_group_if_asked(texto)

    # 2) Categoria + grupo sugeridos pelas palavras
    categoria, grupo_sugerido, kw_cat = _guess_categoria_grupo(texto)

    # 3) Grupo final (forçado tem prioridade)
    grupo = grupo_forcado or grupo_sugerido

    # 4) Tipo a partir do grupo
    tipo = _find_tipo_por_grupo(grupo)

    forma = _find_forma(texto)
    condicao = _find_condicao(texto)
    descricao = _extrair_descricao(texto, kw_cat)

    row = [
        data_iso,         # Data
        tipo,             # Tipo (Entrada/Saída)
        grupo,            # Grupo (um dos 8)
        categoria,        # Categoria
        descricao,        # Descrição (opcional)
        float(valor),     # Valor
        forma,            # Forma de pgto
        condicao,         # Condição de pgto
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
        await tg_send(chat_id, "Mande algo como: 'gastei mercado 120 hoje no cartão' ou '💵 recebi salário 3500'.")
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
                f"Categoria: {row[3]}\nDescrição: {row[4] or '(vazia)'}\n"
                f"Valor: {row[5]:.2f}\nForma: {row[6]}\nCondição: {row[7]}"
            )

        excel_add_row(row)
        await tg_send(chat_id, "✅ Lançado!")
    except Exception as e:
        await tg_send(chat_id, f"❌ Erro: {e}")

    return {"ok": True}
