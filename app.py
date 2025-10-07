import os
import re
import logging
import unicodedata
from datetime import datetime, timedelta

from fastapi import FastAPI, Request
import requests
import httpx
import msal

# ===== Logging =====
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# ===== Telegram =====
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

# ===== Graph / Excel =====
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Sanitiza caminho e define defaults
EXCEL_PATH = (os.getenv("EXCEL_PATH") or "").strip()  # /users/.../drive/items/ID  OU  /users/.../drive/root:/Documents/Planilhas/Lancamentos.xlsx
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Plan1")
TABLE_NAME = os.getenv("TABLE_NAME", "Lancamentos")
SCOPE = ["https://graph.microsoft.com/.default"]

# ===== MSAL / Graph helpers =====
def msal_token():
    """Token app-only (client credentials) para Microsoft Graph."""
    if not (TENANT_ID and CLIENT_ID and CLIENT_SECRET):
        raise RuntimeError("Credenciais MSAL ausentes (TENANT_ID/CLIENT_ID/CLIENT_SECRET).")
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

def excel_add_row(values):
    """
    Adiciona uma linha na Tabela do Excel.
    Suporta EXCEL_PATH:
      1) Por caminho: /users/.../drive/root:/Documents/Planilhas/Lancamentos.xlsx  → usa ':/workbook/...'
      2) Por ID:      /users/.../drive/items/01ABC...!123                        → usa '/workbook/...'
    """
    if not EXCEL_PATH:
        raise RuntimeError("EXCEL_PATH não definido nas variáveis de ambiente.")

    token = msal_token()

    if "/drive/items/" in EXCEL_PATH:
        graph_url = (
            f"{GRAPH_BASE}{EXCEL_PATH}"
            f"/workbook/worksheets('{WORKSHEET_NAME}')/tables('{TABLE_NAME}')/rows/add"
        )
    else:
        graph_url = (
            f"{GRAPH_BASE}{EXCEL_PATH}"
            f":/workbook/worksheets('{WORKSHEET_NAME}')/tables('{TABLE_NAME}')/rows/add"
        )

    payload = {"values": [values]}
    r = requests.post(
        graph_url,
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=25,
    )
    if r.status_code >= 300:
        logger.error("Graph error %s. URL: %s. Resp: %s", r.status_code, graph_url, r.text)
        raise RuntimeError(f"Graph error {r.status_code}: url={graph_url} resp={r.text}")
    return r.json()

# ===== Parsing util =====
ADD_CMD = re.compile(r"^\/?add\b", re.IGNORECASE)
MONEY_RE = re.compile(r"(?:r\$\s*)?(\d{1,3}(?:\.\d{3})*,\d{2}|\d+[,\.]\d{2})", re.IGNORECASE)
DATE_ANY_RE = re.compile(r"(\b\d{1,2}[\/\.-]\d{1,2}(?:[\/\.-]\d{2,4})?\b|\bhoje\b|\bontem\b)", re.IGNORECASE)

def _strip_accents(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")

def _to_float_br(num_str: str) -> float:
    s = num_str.strip().replace(" ", "")
    s = s.replace("R$", "").replace("r$", "").strip()
    s = s.replace(".", "").replace(",", ".")
    return float(s)

def _parse_semicolon(payload: str):
    parts = [p.strip() for p in payload.split(";")]
    if len(parts) == 6:
        return parts
    return None

def _parse_space6(payload: str):
    """
    Formato estruturado por espaços, só se o primeiro token for uma data:
    /add DD/MM[/AAAA] Tipo Categoria Descricao Valor Forma
    """
    toks = payload.split()
    if len(toks) >= 6:
        first = toks[0]
        # aceita DD/MM, DD-MM, DD.MM, DD/MM/AAAA, etc.
        if re.match(r"^\d{1,2}[\/\.-]\d{1,2}([\/\.-]\d{2,4})?$", first):
            data = first
            tipo = toks[1]
            categoria = toks[2]
            desc = " ".join(toks[3:-2]).replace("_", " ")
            valor = toks[-2]
            forma = toks[-1]
            return [data, tipo, categoria, desc, valor, forma]
    return None

def _normalize_date_br(s: str) -> str:
    """
    Aceita: 'DD/MM', 'DD/MM/AAAA', 'DD-MM', 'DD-MM-AAAA', 'DD.MM', 'hoje', 'ontem'.
    Retorna sempre 'DD/MM/AAAA' (ano atual quando faltando).
    """
    txt = (s or "").strip().lower()
    hoje = datetime.now()
    if txt == "hoje":
        return hoje.strftime("%d/%m/%Y")
    if txt == "ontem":
        return (hoje - timedelta(days=1)).strftime("%d/%m/%Y")

    # normaliza separadores
    txt = txt.replace("-", "/").replace(".", "/")
    # extrai grupos com regex robusta
    m = re.search(r"(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?$", txt)
    if not m:
        return s
    d, mth, y = m.group(1), m.group(2), m.group(3)
    if not y:
        y = str(hoje.year)
    elif len(y) == 2:
        y = f"20{y}"
    return f"{int(d):02d}/{int(mth):02d}/{int(y):04d}"

def _force_date_ddmmyyyy(s: str) -> str:
    """
    Força a extração da primeira data válida do texto e retorna DD/MM/AAAA.
    """
    if not s:
        return s
    txt = s.strip().lower().replace("-", "/").replace(".", "/")
    m = re.search(r"(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?", txt)
    if m:
        d, mth, y = m.group(1), m.group(2), m.group(3)
        if not y:
            y = str(datetime.now().year)
        elif len(y) == 2:
            y = f"20{y}"
        return f"{int(d):02d}/{int(mth):02d}/{int(y):04d}"
    return s

def _clean_text_for_freeform(text: str) -> str:
    """
    Limpa ruídos: remove acentos, injeta espaço após 'R$', remove vírgulas/pontos soltos, normaliza espaços.
    """
    t = text.replace("R$", "R$ ").replace("r$", "r$ ")
    t = _strip_accents(t.lower())
    t = re.sub(r"([a-z0-9]+)[\.,](\s|$)", r"\1\2", t, flags=re.IGNORECASE)  # "cartão," -> "cartao"
    t = re.sub(r"\s+", " ", t).strip()
    return t

# Mapa simples de palavras-chave → categoria
CATEGORIA_MAP = {
    # alimentação / mercado
    "mercado": "Alimentação", "supermercado": "Alimentação", "padaria": "Alimentação",
    "restaurante": "Alimentação", "almoco": "Alimentação", "almoço": "Alimentação",
    # moradia / contas
    "aluguel": "Moradia", "luz": "Contas", "agua": "Contas", "internet": "Contas",
    # transporte
    "combustivel": "Transporte", "gasolina": "Transporte", "uber": "Transporte", "taxi": "Transporte",
    # saúde
    "farmacia": "Saúde",
}

def _guess_categoria(t: str) -> str:
    for kw, cat in CATEGORIA_MAP.items():
        if re.search(rf"\b{kw}\b", t):
            return cat
    return "Geral"

def _guess_forma(t: str) -> str:
    if re.search(r"\bpix\b", t): return "Pix"
    if re.search(r"\bdinheiro\b", t): return "Dinheiro"
    if re.search(r"\bboleto\b", t): return "Boleto"
    if re.search(r"\b(transferencia|ted|doc)\b", t): return "Transferência"
    if re.search(r"\bdebito\b", t): return "Cartão"
    if re.search(r"\bcredito\b", t): return "Cartão"
    if re.search(r"\bcartao\b", t): return "Cartão"
    return "Cartão"

def _parse_freeform(text: str):
    """
    Frase natural, ex:
    "gastei 45,90 no mercado com cartão, almoço do time dia 07/10/2025"
    Retorna [data, tipo, categoria, descricao, valor, forma] (strings)
    """
    original = text
    t = _clean_text_for_freeform(text)

    # data (inclui hoje/ontem). se não achar, assume hoje
    m_data = DATE_ANY_RE.search(t)
    if m_data:
        data_br = _normalize_date_br(m_data.group(1))
    else:
        data_br = datetime.now().strftime("%d/%m/%Y")

    # valor (obrigatório p/ lançar)
    m_valor = MONEY_RE.search(t)
    if not m_valor:
        return None
    valor_str = m_valor.group(1)

    # forma / tipo / categoria
    forma = _guess_forma(t)
    tipo = "Receita" if re.search(r"\b(receita|entrada|recebi|venda|ganhei)\b", t) else "Compra"
    categoria = _guess_categoria(t)

    # descrição básica a partir do original, removendo valor e a data encontrada
    desc = re.sub(ADD_CMD, "", original, flags=re.IGNORECASE).strip()
    if m_data:
        desc = re.sub(m_data.group(1), "", desc, flags=re.IGNORECASE)
    desc = re.sub(r"R\$\s*", "", desc, flags=re.IGNORECASE)
    desc = desc.replace(m_valor.group(0), "")  # valor com ou sem R$
    desc = " ".join(desc.split()).strip(" ,.-")
    if not desc:
        desc = f"{tipo} {categoria}"

    return [data_br, tipo, categoria, desc, valor_str, forma]

def _fallback_quick_parse(text: str):
    """
    Plano B: se o parser natural falhar, tenta extrair o valor e usa defaults.
    Retorna lista compatível ou None se nem valor achar.
    """
    t = _clean_text_for_freeform(text)
    m_valor = MONEY_RE.search(t)
    if not m_valor:
        return None

    valor_str = m_valor.group(1)
    m_data = DATE_ANY_RE.search(t)
    data_br = _normalize_date_br(m_data.group(1)) if m_data else datetime.now().strftime("%d/%m/%Y")

    forma = _guess_forma(t)
    tipo = "Receita" if re.search(r"\b(receita|entrada|recebi|venda|ganhei)\b", t) else "Compra"
    categoria = _guess_categoria(t)

    desc = re.sub(ADD_CMD, "", text, flags=re.IGNORECASE).strip()
    if m_data:
        desc = re.sub(m_data.group(1), "", desc, flags=re.IGNORECASE)
    desc = re.sub(r"R\$\s*", "", desc, flags=re.IGNORECASE)
    desc = desc.replace(m_valor.group(0), "")
    desc = " ".join(desc.split()).strip(" ,.-")
    if not desc:
        desc = f"{tipo} {categoria}"

    return [data_br, tipo, categoria, desc, valor_str, forma]

def parse_add(text: str):
    """
    Aceita:
      - "/add DD/MM[/AAAA];Tipo;Categoria;Descricao;Valor;Forma"
      - "/add DD/MM[/AAAA] Tipo Categoria Descricao Valor Forma"
      - Frase natural (gastei X no Y com Z dia DD/MM[/AAAA] ou 'hoje'/'ontem')
    Retorna: [DataISO, Tipo, Categoria, Descricao, ValorFloat, Forma, Origem]
    """
    raw = text.strip()
    is_cmd = bool(ADD_CMD.match(raw))
    payload = ADD_CMD.sub("", raw, count=1).strip() if is_cmd else raw

    if is_cmd:
        parts = _parse_semicolon(payload) or _parse_space6(payload)
        if parts:
            data_br, tipo, categoria, descricao, valor_str, forma = parts
            data_br = _normalize_date_br(data_br)
        else:
            fr = _parse_freeform(payload) or _fallback_quick_parse(payload)
            if not fr:
                return None, ("Não entendi. Exemplos:\n"
                              "• gastei 45,90 no mercado com cartão hoje\n"
                              "• /add 07/10 compra mercado almoço_do_time 45,90 cartão\n"
                              "• /add 07/10/2025;Compra;Mercado;Almoço do time;45,90;Cartão")
            data_br, tipo, categoria, descricao, valor_str, forma = fr
    else:
        fr = _parse_freeform(payload) or _fallback_quick_parse(payload)
        if not fr:
            return None, ("Me diga pelo menos um valor, ex.: 'gastei 45,90 mercado cartão hoje'.")
        data_br, tipo, categoria, descricao, valor_str, forma = fr

    # força dd/mm/aaaa
    data_br = _force_date_ddmmyyyy(data_br)

    # Data → ISO
    try:
        dt = datetime.strptime(data_br, "%d/%m/%Y")
        data_iso = dt.strftime("%Y-%m-%d")
    except Exception as e:
        logger.error("Falha ao parsear data. data_br='%s' erro=%s", data_br, e)
        return None, f"Data inválida: '{data_br}'. Use DD/MM/AAAA (ex.: 07/10/2025)."

    # Valor
    try:
        valor = _to_float_br(valor_str)
    except Exception:
        return None, "Valor inválido. Ex.: 123,45"

    origem = "Telegram"
    return [data_iso, tipo.title(), categoria, descricao, valor, forma, origem], None

# ===== Telegram send =====
async def tg_send(chat_id, text):
    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN ausente.")
        return
    async with httpx.AsyncClient(timeout=12) as client:
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
    try:
        body = await req.json()
        message = body.get("message") or {}
        chat_id = message.get("chat", {}).get("id")
        text = (message.get("text") or "").strip()

        if not chat_id or not text:
            return {"ok": True}

        if text.lower().startswith("/start"):
            await tg_send(chat_id, "Olá! Envie frases como: 'gastei 45,90 no mercado com cartão hoje'.")
            return {"ok": True}

        # Dispara parser para /add OU qualquer mensagem que pareça ter valor/data
        lower_txt = text.lower()
        has_money = bool(MONEY_RE.search(lower_txt))
        looks_structured_date = bool(re.match(r"^\d{1,2}[\/\.-]\d{1,2}([\/\.-]\d{2,4})?\b", lower_txt))
        if ADD_CMD.match(text) or has_money or looks_structured_date or any(ch.isdigit() for ch in lower_txt):
            row, err = parse_add(text)
            if err:
                await tg_send(chat_id, f"❗ {err}")
                return {"ok": True}
            try:
                excel_add_row(row)
                # Resposta genérica
                await tg_send(chat_id, "✅ Lançado!")
            except Exception as e:
                logger.exception("Falha ao escrever no Excel")
                await tg_send(chat_id, f"❌ Erro ao lançar no Excel: {e}")
            return {"ok": True}

        # Se não for nada disso, ignora silenciosamente
        return {"ok": True}

    except Exception:
        logger.exception("Erro inesperado no webhook")
        return {"ok": True}
