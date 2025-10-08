import os
import re
import unicodedata
import requests
import httpx
import msal
from datetime import datetime, timedelta
from fastapi import FastAPI, Request, Header
from typing import Optional, Dict, Any, List

app = FastAPI()

# === CONFIG (Telegram) ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

# === CONFIG (Graph) ===
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SCOPE = ["https://graph.microsoft.com/.default"]

# === FALLBACK (opcional para teste único) ===
EXCEL_PATH_FALLBACK = (os.getenv("EXCEL_PATH") or "").strip()
WORKSHEET_NAME_FALLBACK = os.getenv("WORKSHEET_NAME", "Plan1")
TABLE_NAME_FALLBACK = os.getenv("TABLE_NAME", "Lancamentos")

# === CLIENTS MAPPING (Planilha Clientes.xlsx) ===
CLIENTS_TABLE_PATH = (os.getenv("CLIENTS_TABLE_PATH") or "").strip()
CLIENTS_WORKSHEET = os.getenv("CLIENTS_WORKSHEET_NAME", "Plan1")
CLIENTS_TABLE = os.getenv("CLIENTS_TABLE_NAME", "Clientes")

# Proteção do painel admin
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

# Debug
DEBUG = os.getenv("DEBUG", "0") == "1"

# Cache simples em memória (chat_id -> mapping)
CLIENT_CACHE: Dict[str, Dict[str, str]] = {}


# =============== MSAL TOKEN ===============
def msal_token() -> str:
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


# =============== GRAPH HELPERS (tabela genérica) ===============
def graph_table_add_row(table_path: str, worksheet: str, table: str, values: List[Any]):
    token = msal_token()
    if "/drive/items/" in table_path:
        url = f"{GRAPH_BASE}{table_path}/workbook/worksheets('{worksheet}')/tables('{table}')/rows/add"
    else:
        url = f"{GRAPH_BASE}{table_path}:/workbook/worksheets('{worksheet}')/tables('{table}')/rows/add"
    payload = {"values": [values]}
    r = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=payload, timeout=25)
    if r.status_code >= 300:
        raise RuntimeError(f"Graph add_row error {r.status_code}: {r.text}")
    return r.json()

def graph_table_list_rows(table_path: str, worksheet: str, table: str) -> List[List[Any]]:
    token = msal_token()
    if "/drive/items/" in table_path:
        url = f"{GRAPH_BASE}{table_path}/workbook/worksheets('{worksheet}')/tables('{table}')/rows"
    else:
        url = f"{GRAPH_BASE}{table_path}:/workbook/worksheets('{worksheet}')/tables('{table}')/rows"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=25)
    if r.status_code >= 300:
        raise RuntimeError(f"Graph list_rows error {r.status_code}: {r.text}")
    data = r.json()
    rows = []
    for item in data.get("value", []):
        vals = item.get("values", [])
        if vals and isinstance(vals, list):
            rows.extend(vals)
    return rows


# =============== EXCEL APPEND PARA LANÇAMENTOS (no fim da tabela) ===============
def excel_add_row(excel_path: str, worksheet_name: str, table_name: str, values: List[Any]):
    token = msal_token()
    if "/drive/items/" in excel_path:
        url = f"{GRAPH_BASE}{excel_path}/workbook/worksheets('{worksheet_name}')/tables('{table_name}')/rows/add"
    else:
        url = f"{GRAPH_BASE}{excel_path}:/workbook/worksheets('{worksheet_name}')/tables('{table_name}')/rows/add"
    payload = {"values": [values]}  # sem index → append no fim
    r = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=payload, timeout=25)
    if r.status_code >= 300:
        raise RuntimeError(f"Graph error {r.status_code}: {r.text}")
    return r.json()


# =============== MAPEAMENTO chat_id -> planilha (via Clientes.xlsx) ===============
def resolve_client_mapping(chat_id: str) -> Optional[Dict[str, str]]:
    """Busca no cache; se não tiver, lê da planilha Clientes.xlsx."""
    if chat_id in CLIENT_CACHE:
        return CLIENT_CACHE[chat_id]

    if not CLIENTS_TABLE_PATH:
        # sem cadastro central → usa fallback (modo 1-usuário)
        if EXCEL_PATH_FALLBACK:
            mapping = {
                "excel_path": EXCEL_PATH_FALLBACK,
                "worksheet_name": WORKSHEET_NAME_FALLBACK,
                "table_name": TABLE_NAME_FALLBACK,
            }
            CLIENT_CACHE[chat_id] = mapping
            return mapping
        return None

    rows = graph_table_list_rows(CLIENTS_TABLE_PATH, CLIENTS_WORKSHEET, CLIENTS_TABLE)
    # Espera colunas: chat_id | excel_path | worksheet_name | table_name | created_at
    for r in rows:
        if not r or len(r) < 4:
            continue
        cid = str(r[0]).strip()
        if cid == chat_id:
            mapping = {
                "excel_path": str(r[1]).strip(),
                "worksheet_name": str(r[2]).strip() or "Plan1",
                "table_name": str(r[3]).strip() or "Lancamentos",
            }
            CLIENT_CACHE[chat_id] = mapping
            return mapping
    return None

def register_client_mapping(chat_id: str, excel_path: str, worksheet_name: str, table_name: str):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    graph_table_add_row(
        CLIENTS_TABLE_PATH,
        CLIENTS_WORKSHEET,
        CLIENTS_TABLE,
        [str(chat_id), excel_path, worksheet_name, table_name, now]
    )
    # Atualiza cache
    CLIENT_CACHE[str(chat_id)] = {
        "excel_path": excel_path.strip(),
        "worksheet_name": worksheet_name.strip() or "Plan1",
        "table_name": table_name.strip() or "Lancamentos",
    }

def upsert_client_placeholder(chat_id: str):
    """
    Se 'chat_id' não existir na tabela Clientes, adiciona uma linha:
    [chat_id, "", "Plan1", "Lancamentos", created_at]
    """
    if not CLIENTS_TABLE_PATH:
        return  # sem planilha de clientes configurada

    rows = graph_table_list_rows(CLIENTS_TABLE_PATH, CLIENTS_WORKSHEET, CLIENTS_TABLE)
    for r in rows:
        if not r or len(r) < 1:
            continue
        if str(r[0]).strip() == str(chat_id):
            return  # já existe

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    graph_table_add_row(
        CLIENTS_TABLE_PATH,
        CLIENTS_WORKSHEET,
        CLIENTS_TABLE,
        [str(chat_id), "", "Plan1", "Lancamentos", now]
    )


# =============== PARSER/CLASSIFICAÇÃO ===============
def _strip_accents(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")

MONEY_RE = re.compile(r"(?:r\$\s*)?(\d{1,3}(?:\.\d{3})*(?:,\d{2})?|\d+(?:[.,]\d{2})?)(?!\S)", re.IGNORECASE)

def _find_valor(texto: str):
    m = MONEY_RE.search(texto.replace("R$","R$ "))
    if not m: return None
    s = m.group(1).strip().replace(".", "").replace(",", ".")
    try: return float(s)
    except: return None

def _find_data(texto: str) -> datetime:
    t = _strip_accents(texto.lower()); hoje = datetime.now()
    if "ontem" in t: return hoje - timedelta(days=1)
    if "hoje" in t: return hoje
    m = re.search(r"(\d{1,2})[\/\.-](\d{1,2})(?:[\/\.-](\d{2,4}))?", t)
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3)
        if not y: y = str(hoje.year)
        elif len(y) == 2: y = f"20{y}"
        try: return datetime.strptime(f"{int(d):02d}/{int(mo):02d}/{int(y):04d}", "%d/%m/%Y")
        except: pass
    return hoje

def _find_forma(texto: str) -> str:
    t = _strip_accents(texto.lower())
    if "pix" in t: return "Pix"
    if "dinheiro" in t: return "Dinheiro"
    if "boleto" in t: return "Boleto"
    m = re.search(r"cart[aã]o(?:\s+(?:de\s+)?([\w\-]+))?", t)
    if m:
        nome = m.group(1)
        if nome: return f"💳 Cartão {nome.capitalize()}"
        return "💳 Cartão"
    if "credito" in t or "crédito" in t: return "💳 Cartão de Crédito"
    if "debito" in t or "débito" in t: return "💳 Cartão de Débito"
    return "💳 Cartão"

def _find_condicao(texto: str) -> str:
    t = _strip_accents(texto.lower())
    m = re.search(r"\b(\d{1,2})\s*x\b", t)
    if m: return f"{int(m.group(1))}x"
    m = re.search(r"\b(\d{1,2})\s*vezes\b", t)
    if m: return f"{int(m.group(1))}x"
    m = re.search(r"parcel\w*\s*(?:em\s*)?(\d{1,2})\s*x", t)
    if m: return f"{int(m.group(1))}x"
    m = re.search(r"parcel\w*\s*(?:em\s*)?(\d{1,2})\s*vezes", t)
    if m: return f"{int(m.group(1))}x"
    if "parcel" in t: return "Parcelado"
    return "À vista"

GROUP_FORCE = {
    "🏠": "Gastos Fixos","gasto fixo": "Gastos Fixos","fixo": "Gastos Fixos","fixa": "Gastos Fixos",
    "📺": "Assinatura","assinatura": "Assinatura","assinaturas": "Assinatura",
    "💸": "Gastos Variáveis","variavel": "Gastos Variáveis","variável": "Gastos Variáveis",
    "🧾": "Despesas Temporárias","despesa temporaria": "Despesas Temporárias","temporaria": "Despesas Temporárias",
    "💳": "Pagamento de Fatura","fatura": "Pagamento de Fatura","cartao fatura": "Pagamento de Fatura",
    "💵": "Ganhos","ganho": "Ganhos","renda": "Ganhos","salario": "Ganhos","salário": "Ganhos",
    "💰": "Investimento","investimento": "Investimento","investi": "Investimento",
    "📝": "Reserva","reserva": "Reserva",
}

CATEGORIAS = {
    # Fixos
    "aluguel": ("Aluguel","Gastos Fixos"),
    "condominio": ("Condomínio","Gastos Fixos"), "condomínio": ("Condomínio","Gastos Fixos"),
    "energia": ("Energia","Gastos Fixos"), "luz": ("Energia","Gastos Fixos"),
    "agua": ("Água","Gastos Fixos"), "água": ("Água","Gastos Fixos"),
    "internet": ("Internet","Gastos Fixos"), "telefone": ("Telefone","Gastos Fixos"),
    "plano de saude": ("Plano de Saúde","Gastos Fixos"), "plano de saúde": ("Plano de Saúde","Gastos Fixos"),
    "academia": ("Academia","Gastos Fixos"), "escola": ("Escola","Gastos Fixos"), "faculdade": ("Faculdade","Gastos Fixos"),

    # Temporárias
    "iptu": ("IPTU","Despesas Temporárias"), "ipva": ("IPVA","Despesas Temporárias"),
    "financiamento": ("Financiamento","Despesas Temporárias"),
    "emprestimo": ("Empréstimo","Despesas Temporárias"), "empréstimo": ("Empréstimo","Despesas Temporárias"),

    # Assinaturas
    "netflix": ("Netflix","Assinatura"), "amazon": ("Amazon Prime","Assinatura"), "prime video": ("Amazon Prime","Assinatura"),
    "disney": ("Disney+","Assinatura"), "disney+": ("Disney+","Assinatura"),
    "hbo": ("HBO Max","Assinatura"), "hbo max": ("HBO Max","Assinatura"), "premiere": ("Premiere","Assinatura"),
    "spotify": ("Spotify","Assinatura"), "youtube premium": ("YouTube Premium","Assinatura"),

    # Variáveis — Alimentação
    "restaurante": ("Restaurante","Gastos Variáveis"), "lanchonete": ("Lanchonete","Gastos Variáveis"),
    "padaria": ("Padaria","Gastos Variáveis"), "bar": ("Bar","Gastos Variáveis"),
    "cafe": ("Café","Gastos Variáveis"), "café": ("Café","Gastos Variáveis"),
    "pizzaria": ("Pizzaria","Gastos Variáveis"), "pastelaria": ("Pastelaria","Gastos Variáveis"),
    "ifood": ("iFood","Gastos Variáveis"),

    # Variáveis — Casa/Compras
    "mercado": ("Mercado","Gastos Variáveis"), "supermercado": ("Mercado","Gastos Variáveis"),
    "acougue": ("Açougue","Gastos Variáveis"), "açougue": ("Açougue","Gastos Variáveis"),
    "hortifruti": ("Hortifruti","Gastos Variáveis"), "feira": ("Feira","Gastos Variáveis"),
    "farmacia": ("Farmácia","Gastos Variáveis"), "farmácia": ("Farmácia","Gastos Variáveis"),

    # Variáveis — Transporte
    "gasolina": ("Gasolina","Gastos Variáveis"), "combustivel": ("Combustível","Gastos Variáveis"), "combustível": ("Combustível","Gastos Variáveis"),
    "uber": ("Uber","Gastos Variáveis"), "99": ("99","Gastos Variáveis"),
    "taxi": ("Táxi","Gastos Variáveis"), "táxi": ("Táxi","Gastos Variáveis"),
    "onibus": ("Ônibus","Gastos Variáveis"), "ônibus": ("Ônibus","Gastos Variáveis"),
    "metro": ("Metrô","Gastos Variáveis"), "metrô": ("Metrô","Gastos Variáveis"),
    "estacionamento": ("Estacionamento","Gastos Variáveis"), "pedagio": ("Pedágio","Gastos Variáveis"), "pedágio": ("Pedágio","Gastos Variáveis"),

    # Variáveis — Saúde
    "medico": ("Médico","Gastos Variáveis"), "médico": ("Médico","Gastos Variáveis"),
    "dentista": ("Dentista","Gastos Variáveis"), "exame": ("Exame","Gastos Variáveis"),
    "hospital": ("Hospital","Gastos Variáveis"), "laboratorio": ("Laboratório","Gastos Variáveis"), "laboratório": ("Laboratório","Gastos Variáveis"),

    # Variáveis — Lazer/Viagem
    "passeio": ("Passeio em família","Gastos Variáveis"), "viagem": ("Viagem","Gastos Variáveis"),
    "hotel": ("Hotel","Gastos Variáveis"), "airbnb": ("Airbnb","Gastos Variáveis"), "passagem": ("Passagem","Gastos Variáveis"),

    # Ganhos
    "salario": ("Salário","Ganhos"), "salário": ("Salário","Ganhos"),
    "vale": ("Vale","Ganhos"), "pro labore": ("Pró labore","Ganhos"), "pró labore": ("Pró labore","Ganhos"),
    "bonus": ("Bônus","Ganhos"), "bônus": ("Bônus","Ganhos"),
    "comissao": ("Comissão","Ganhos"), "comissão": ("Comissão","Ganhos"),
    "renda extra": ("Renda Extra","Ganhos"), "renda extra 1": ("Renda Extra 1","Ganhos"), "renda extra 2": ("Renda Extra 2","Ganhos"),

    # Investimento
    "renda fixa": ("Renda Fixa","Investimento"), "renda variavel": ("Renda Variável","Investimento"), "renda variável": ("Renda Variável","Investimento"),
    "acoes": ("Ações","Investimento"), "ações": ("Ações","Investimento"), "bolsa": ("Ações","Investimento"),
    "cdb": ("CDB","Investimento"), "lci": ("LCI","Investimento"), "lca": ("LCA","Investimento"),
    "fii": ("Fundos imobiliários","Investimento"), "fundos imobiliarios": ("Fundos imobiliários","Investimento"), "fundos imobiliários": ("Fundos imobiliários","Investimento"),

    # Reserva
    "trocar de carro": ("Trocar de carro","Reserva"), "viagem pra disney": ("Viagem pra Disney","Reserva"),
    "emergencia": ("Reserva de Emergência","Reserva"), "emergência": ("Reserva de Emergência","Reserva"),
}

FIXO_HINTS = {"mensal","mensalidade","assinatura","plano","aluguel","condominio","condomínio","luz","energia","agua","água","internet","telefone","iptu","ipva","academia","escola","faculdade"}

def _force_group_if_asked(texto: str):
    t = _strip_accents(texto.lower())
    for k, g in GROUP_FORCE.items():
        if _strip_accents(k) in t: return g
    return None

def _guess_categoria_grupo(texto: str):
    t = _strip_accents(texto.lower())
    for kw, (cat, grp) in CATEGORIAS.items():
        if kw in t: return cat, grp, kw
    if any(w in t for w in ["recebi","ganhei","entrada","venda","salario","salário","vale","bônus","bonus","comissao","comissão"]):
        return "Ganhos","Ganhos",None
    if any(h in t for h in FIXO_HINTS):
        return "Outros","Gastos Fixos",None
    return "Outros","Gastos Variáveis",None

def _find_tipo_por_grupo(grupo: str) -> str:
    return "Entrada" if grupo == "Ganhos" else "Saída"

STOP_PREPS = {"na","no","em","de","do","da","para","pra","por","via","com","sem","ao","a","o","os","as","um","uma","uns","umas"}
VERBOS_GASTO = {"gastei","paguei","comprei","compra","pago","investi","reservei","guardei","pague"}
PAG_PALAVRAS = {"pix","dinheiro","boleto","debito","credito","cartao","débito","crédito","cartão"}
TEMPO_PALAVRAS = {"hoje","ontem"}

def _extrair_descricao(original: str, kw_cat):
    txt = _strip_accents(original.lower())
    txt = MONEY_RE.sub(" "," ")
    txt = re.sub(r"\b\d{1,2}[\/\.-]\d{1,2}(?:[\/\.-](\d{2,4}))?\b"," ", txt)
    for w in TEMPO_PALAVRAS | VERBOS_GASTO | PAG_PALAVRAS | STOP_PREPS:
        txt = re.sub(rf"\b{w}\b"," ", txt)
    if kw_cat: txt = re.sub(rf"\b{kw_cat}\b"," ", txt)
    for k in GROUP_FORCE.keys():
        txt = re.sub(re.escape(_strip_accents(k))," ", txt)
    txt = re.sub(r"\b\d{1,2}x\b"," ", txt)
    txt = re.sub(r"[^\w\s]"," ", txt)
    txt = re.sub(r"\s+"," ", txt).strip()
    if not txt or (kw_cat and txt == kw_cat): return ""
    return txt[:60]

def interpretar_frase(texto: str):
    texto = (texto or "").strip()
    valor = _find_valor(texto)
    if valor is None: return None, "Não encontrei o valor na mensagem."
    data_dt = _find_data(texto); data_iso = data_dt.strftime("%Y-%m-%d")
    grupo_forcado = _force_group_if_asked(texto)
    categoria, grupo_sugerido, kw_cat = _guess_categoria_grupo(texto)
    grupo = grupo_forcado or grupo_sugerido
    tipo = _find_tipo_por_grupo(grupo)
    forma = _find_forma(texto)
    condicao = _find_condicao(texto)
    descricao = _extrair_descricao(texto, kw_cat)
    row = [data_iso, tipo, grupo, categoria, descricao, float(valor), forma, condicao]
    return row, None


# =============== TELEGRAM SEND ===============
async def tg_send(chat_id, text):
    async with httpx.AsyncClient(timeout=12) as client:
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text},
        )


# =============== ROUTES ===============
@app.get("/")
def root():
    return {"status": "ok"}

@app.post("/telegram/webhook")
async def telegram_webhook(req: Request):
    body = await req.json()
    msg = body.get("message", {})
    chat_id = str(msg.get("chat", {}).get("id") or "").strip()
    text = (msg.get("text") or "").strip()

    if not chat_id or not text:
        return {"ok": True}

    # /id para o usuário consultar o próprio chat_id
    if text.lower().startswith("/id"):
        await tg_send(chat_id, f"Seu chat_id é: {chat_id}")
        return {"ok": True}

    if text.lower().startswith("/start"):
        # 1) anota o chat_id como placeholder na Clientes.xlsx (se configurado)
        try:
            upsert_client_placeholder(chat_id)
        except Exception as e:
            if DEBUG:
                await tg_send(chat_id, f"[DEBUG] Falha ao anotar seu chat_id em Clientes.xlsx: {e}")

        # 2) mensagem de boas-vindas
        await tg_send(
            chat_id,
            "Olá! Seu cadastro foi iniciado. 👋\n"
            f"Seu chat_id é: {chat_id}\n"
            "Assim que o administrador vincular sua planilha, você já poderá lançar gastos.\n\n"
            "Ex.: 'gastei 104 no restaurante bela italia hoje, via pix'"
        )
        return {"ok": True}

    # localizar planilha do cliente
    mapping = resolve_client_mapping(chat_id)
    if not mapping or not mapping.get("excel_path"):
        await tg_send(chat_id, "❗ Seu chat ainda não está vinculado a uma planilha. Peça ao administrador para fazer o cadastro.")
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

        excel_add_row(mapping["excel_path"], mapping["worksheet_name"], mapping["table_name"], row)
        await tg_send(chat_id, "✅ Lançado!")
    except Exception as e:
        await tg_send(chat_id, f"❌ Erro: {e}")

    return {"ok": True}


# =============== ADMIN (cadastro manual) ===============
def _auth_admin(auth_header: Optional[str]) -> bool:
    if not ADMIN_TOKEN:
        return False
    if not auth_header or not auth_header.startswith("Bearer "):
        return False
    token = auth_header.split(" ", 1)[1].strip()
    return token == ADMIN_TOKEN

@app.post("/admin/register")
async def admin_register(req: Request, authorization: Optional[str] = Header(None)):
    if not _auth_admin(authorization):
        return {"ok": False, "error": "unauthorized"}
    if not CLIENTS_TABLE_PATH:
        return {"ok": False, "error": "CLIENTS_TABLE_PATH não configurado"}

    data = await req.json()
    chat_id = str(data.get("chat_id") or "").strip()
    excel_path = (data.get("excel_path") or "").strip()
    worksheet_name = (data.get("worksheet_name") or "Plan1").strip()
    table_name = (data.get("table_name") or "Lancamentos").strip()

    if not chat_id or not excel_path:
        return {"ok": False, "error": "chat_id e excel_path são obrigatórios"}

    try:
        register_client_mapping(chat_id, excel_path, worksheet_name, table_name)
        return {"ok": True, "chat_id": chat_id, "excel_path": excel_path}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/admin/clients")
def admin_clients(authorization: Optional[str] = Header(None)):
    if not _auth_admin(authorization):
        return {"ok": False, "error": "unauthorized"}
    if not CLIENTS_TABLE_PATH:
        return {"ok": False, "error": "CLIENTS_TABLE_PATH não configurado"}

    try:
        rows = graph_table_list_rows(CLIENTS_TABLE_PATH, CLIENTS_WORKSHEET, CLIENTS_TABLE)
        result = []
        for r in rows:
            if not r: continue
            obj = {
                "chat_id": r[0] if len(r) > 0 else "",
                "excel_path": r[1] if len(r) > 1 else "",
                "worksheet_name": r[2] if len(r) > 2 else "",
                "table_name": r[3] if len(r) > 3 else "",
                "created_at": r[4] if len(r) > 4 else "",
            }
            result.append(obj)
        return {"ok": True, "clients": result}
    except Exception as e:
        return {"ok": False, "error": str(e)}
