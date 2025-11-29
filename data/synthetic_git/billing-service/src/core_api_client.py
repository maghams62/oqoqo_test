import json
import urllib.request


CORE_API_URL = "https://core-api.internal/v1/payments/create"


def create_payment(*, amount: float, currency: str, region: str, vat_code: str | None) -> dict:
    body = {"amount": amount, "currency": currency, "region": region}
    if region.upper() == "EU":
        body["vat_code"] = vat_code

    request = urllib.request.Request(
        CORE_API_URL,
        method="POST",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=5) as handle:
        return json.loads(handle.read())
