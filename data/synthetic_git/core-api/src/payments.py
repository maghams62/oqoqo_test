"""Core payments workflow with VAT enforcement for EU regions."""


EU_CURRENCIES = {"EUR", "SEK", "DKK", "NOK"}


def create_payment(
    amount: float,
    currency: str,
    *,
    region: str = "US",
    vat_code: str | None = None,
) -> dict:
    """Require vat_code for EU customers to satisfy compliance."""
    if amount <= 0:
        raise ValueError("amount must be positive")

    upper_region = region.upper()
    currency = currency.upper()

    if upper_region == "EU" or currency in EU_CURRENCIES:
        if not vat_code:
            raise ValueError("vat_code is required for EU payments")

    payload = {
        "amount": amount,
        "currency": currency,
        "region": upper_region,
        "status": "pending",
        "requires_vat_code": upper_region == "EU" or currency in EU_CURRENCIES,
    }
    if vat_code:
        payload["vat_code"] = vat_code
    return payload
