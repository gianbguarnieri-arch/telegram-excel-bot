# --- Atualiza o e-mail no Google Sheets (aba de licenças) ---
LICENSE_SHEET_ID  = os.getenv("LICENSE_SHEET_ID")   # já deve estar setado
LICENSE_SHEET_TAB = os.getenv("LICENSE_SHEET_TAB", "Licencas")  # nome da aba

def sheet_update_email(license_key: str, email: str):
    """
    Localiza a linha pela coluna 'Licenca' e atualiza a coluna 'email'.
    Cabeçalho esperado (linha 1): Licenca | Validade | Data de inicio | Data final | email | status
    Aceita variações de maiúsc/minúsc pois normalizamos para lower().
    """
    if not LICENSE_SHEET_ID or not LICENSE_SHEET_TAB:
        return  # proteção silenciosa

    _, sheets = google_services()

    # Lê cabeçalho + dados
    rng = f"{LICENSE_SHEET_TAB}!A1:F"
    resp = sheets.spreadsheets().values().get(
        spreadsheetId=LICENSE_SHEET_ID,
        range=rng
    ).execute()
    values = resp.get("values", [])
    if not values:
        return

    header = values[0]
    header_lower = [h.strip().lower() for h in header]

    try:
        col_idx_lic = header_lower.index("licenca")
        col_idx_email = header_lower.index("email")
    except ValueError:
        # Cabeçalho inesperado
        return

    # Procura a linha pela coluna Licenca
    row_idx_found = None
    for i, row in enumerate(values[1:], start=2):  # dados começam na linha 2
        if len(row) > col_idx_lic and row[col_idx_lic].strip() == license_key.strip():
            row_idx_found = i
            break
    if not row_idx_found:
        return

    # Converte índice da coluna (0-based) para letra A,B,C...
    def _col_letter(idx0: int) -> str:
        idx = idx0 + 1
        out = ""
        while idx:
            idx, r = divmod(idx - 1, 26)
            out = chr(65 + r) + out
        return out

    email_col_letter = _col_letter(col_idx_email)
    cell_range = f"{LICENSE_SHEET_TAB}!{email_col_letter}{row_idx_found}"

    sheets.spreadsheets().values().update(
        spreadsheetId=LICENSE_SHEET_ID,
        range=cell_range,
        valueInputOption="RAW",
        body={"values": [[email]]}
    ).execute()
