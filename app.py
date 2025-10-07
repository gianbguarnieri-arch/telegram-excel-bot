import os
import re
import logging
import unicodedata
from datetime import datetime

from fastapi import FastAPI, Request
import requests
import httpx
import msal

# ===== Logging básico =====
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

# ⚙️ Sanitiza espaços e quebras de linha
EXCEL_PATH = (os.getenv("EXCEL_PATH") or "").strip()
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Plan1")
TABLE_NAME = os.getenv("TABLE_NAME", "Lancamentos")
SCOPE = ["https://graph.microsoft.com/.default"]


# ===== Helpers =====
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
    Suporta EXCEL_PATH em dois formatos:
      1) Por caminho (rota): /users/.../drive/root:/Documents/Planilhas/Lancamentos.xlsx
         → usa ':/workbook/...'
      2) Por ID:             /users/.../drive/items/01ABC...!123
         → usa '/workbook/...'
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


# ===== Parser “frase natural” + compat =====
ADD_CMD = re.compile(r"^\/?add\b", re.IGNORECASE)

def _strip_accents(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")

def _to_float_br(num_str: str) -> float:
    # aceita "1.234,56" ou "1234,56" ou "1234.56"
    s = num_str.strip().replace(" ", "")
    s = s.replace(".", "").replace(",", ".")
    return float(s)

def _parse_semicolon(payload: str):
    parts = [p.strip() for p in payload.split(";")]
    if len(parts) == 6:
        return parts
    return None

def _parse_space6(payload: str):
    # /add DD/MM/AAAA Tipo Categoria Descricao Valor Forma
    # para permitir espaços na descrição, usuário pode usar _ (almoço_do_time)
    toks = payload.split()
    if len(toks) >= 6:
        # une “sobras” no meio para sempre fechar 6 campos
        data = toks[0]
        tipo = toks[1]
        categoria = toks[2]
        # descrição pode ter vários tokens; pega tudo até o penúltimo
        desc = " ".join(toks[3:-2]).replace("_", " ")
        valor = toks[-2]
        forma = toks[-1]
        return [data, tipo, categoria, desc, valor, forma]
    return None

def _parse_freeform(text: str):
    """
    Frase natural, ex:
    "gastei 45,90 no mercado com cartão, almoço do time dia 07/10/2025"
    Retorna [data, tipo, categoria, descricao, valor, forma] (strings)
    """
    original = text
    t = _strip_accents(text.lower())

    # 1) data (dd/mm ou dd/mm/aaaa)
    m_data = re.search(r"(\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b)", t)
    if m_data:
        data_br = m_data.group(1)
        if len(data_br.split("/")) == 2:
            # completa com ano atual
            data_br = f"{data_br}/{datetime.now().year}"
    else:
        data_br = datetime.now().strftime("%d/%m/%Y")

    # 2) valor (pega a 1ª ocorrência que pareça dinheiro)
    m_valor = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2}|\d+[,\.]\d{2})", t)
    if not m_valor:
        return None  # sem valor não dá
    valor_str = m_valor.group(1)

    # 3) forma de pagamento
    formas = {
        "pix": "Pix",
        "cartao": "Cartão",
        "credito": "Cartão",
        "debito": "Cartão",
        "dinheiro": "Dinheiro",
        "boleto": "Boleto",
        "transferencia": "Transferência",
        "ted": "Transferência",
        "doc": "Transferência",
    }
    forma = "Cartão"
    for k, v in formas.items():
        if re.search(rf"\b{k}\b", t):
            forma = v
            break

    # 4) tipo (receita/compra)
    if re.search(r"\b(receita|entrada|recebi|venda|ganhei)\b", t):
        tipo = "Receita"
    else:
        tipo = "Compra"

    # 5) categoria (tentativa simples por palavras)
    categorias_vocab = [
        "mercado", "supermercado", "farmacia", "combustivel", "gasolina",
        "restaurante", "almoço", "almoco", "taxi", "uber", "aluguel",
        "luz", "agua", "internet", "padaria"
    ]
    categoria = "Geral"
    for kw in categorias_vocab:
        if re.search(rf"\b{kw}\b", t):
            categoria = "Mercado" if kw in ("mercado", "supermercado") else kw.capitalize()
            break

    # 6) descrição = frase original menos data/valor/pagamentos gatilhos
    # remove /add e vírgulas supérfluas
    desc = re.sub(ADD_CMD, "", original, flags=re.IGNORECASE).strip()
    # remove data/valor ocorrências
    desc = re.sub(m_data.group(1), "", desc) if m_data else desc
    desc = desc.replace(valor_str, "")
    # remove palavras comuns de ligação
    remove_words = ["gastei", "paguei", "com", "no", "na", "do", "da", "de", "para", "pra", "o", "a", "no dia", "dia"]
    for w in remove_words + list(formas.keys()):
        desc = re.sub(rf"\b{w}\b", "", _strip_accents(desc).lower(), flags=re.IGNORECASE)
    # volta capitalização simples
    desc = " ".join(tok for tok in desc.replace("  ", " ").strip().split())
    if not desc:
        desc = f"{tipo} {categoria}"

    return [data_br, tipo, categoria, desc, valor_str, forma]

def parse_add(text):
    """
    Aceita:
      - "/add DD/MM/AAAA;Tipo;Categoria;Descricao;Valor;Forma"
      - "/add DD/MM/AAAA Tipo Categoria Descricao Valor Forma"
      - Frase natural (gastei X no Y com Z dia DD/MM[/AAAA])
    Retorna lista final no formato: [DataISO, Tipo, Categoria, Descricao, ValorFloat, Forma, Origem]
    """
    raw = text.strip()

    # tira o prefixo /add (se houver)
    if ADD_CMD.match(raw):
        payload = ADD_CMD.sub("", raw, count=1).strip()
    else:
        payload = raw

    # 1) tentativas estruturadas
    parts = _parse_semicolon(payload) or _parse_space6(payload)
    if parts:
        data_br, tipo, categoria, descricao, valor_str, forma = parts
    else:
        # 2) frase natural
        fr = _parse_freeform(payload)
        if not fr:
            return None, ("Não entendi. Você pode escrever assim:\n"
                          "• gastei 45,90 no mercado com cartão, almoço do time dia 07/10/2025\n"
                          "ou ainda:\n"
                          "• /add 07/10/2025;Compra;Mercado;Almoço do time;45,90;Cartão")
        data_br, tipo, categoria, descricao, valor_str, forma = fr

    # Data → ISO
    try:
        dt = datetime.strptime(data_br, "%d/%m/%Y")
        data_iso = dt.strftime("%Y-%m-%d")
    except Exception:
        return None, "Data inválida. Use DD/MM/AAAA."

    # Valor (vírgula→ponto)
    try:
        valor = _to_float_br(valor_str)
    except Exception:
        return None, "Valor inválido. Ex.: 123,45"

    origem = "Telegram"
    return [data_iso, tipo.title(), categoria, descricao, valor, forma, origem], None


# ===== Envio de mensagens ao Telegram =====
async def tg_send(chat_id, text):
    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN ausente.")
        return
    async with httpx.AsyncClient(timeout=12) as client:
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text},
        )


# ===== Rotas =====
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
            reply = (
                "Bora lançar seus gastos!\n\n"
                "Você pode escrever de forma natural, por ex.:\n"
                "• gastei 45,90 no mercado com cartão, almoço do time dia 07/10/2025\n\n"
                "Ou usar os formatos estruturados:\n"
                "• /add 07/10/2025 compra mercado almoço_do_time 45,90 cartão\n"
                "• /add 07/10/2025;Compra;Mercado;Almoço do time;45,90;Cartão"
            )
            await tg_send(chat_id, reply)
            return {"ok": True}

        # /add (ou frase contendo 'add')
        if ADD_CMD.match(text) or text.lower().startswith(("gastei", "paguei", "recebi", "ganhei")):
            row, err = parse_add(text)
            if err:
                await tg_send(chat_id, f"❗ {err}")
                return {"ok": True}
            try:
                excel_add_row(row)
                data_iso, tipo, categoria, descricao, valor, forma, _ = row
                msg_ok = (f"✅ Lançado!\n"
                          f"{tipo} • {categoria}\n"
                          f"R$ {valor:.2f} • {forma}\n"
                          f"{descricao}\n"
                          f"📅 {data_iso}")
                await tg_send(chat_id, msg_ok)
            except Exception as e:
                logger.exception("Falha ao escrever no Excel")
                await tg_send(chat_id, f"❌ Erro ao lançar no Excel: {e}")
            return {"ok": True}

        # fallback silencioso
        return {"ok": True}

    except Exception:
        logger.exception("Erro inesperado no webhook")
        return {"ok": True}
