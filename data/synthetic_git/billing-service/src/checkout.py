from .core_api_client import create_payment


def checkout(cart):
    region = "EU" if cart.currency in {"EUR", "SEK"} else "US"
    vat_code = cart.tax_profile.vat_code if region == "EU" else None

    payload = create_payment(
        amount=cart.total_with_discounts(),
        currency=cart.currency,
        region=region,
        vat_code=vat_code,
    )

    if cart.requires_invoice():
        payload["invoice_id"] = cart.invoice_id

    return {"status": "ok", "payment": payload}
