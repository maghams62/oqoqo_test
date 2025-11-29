import json
import urllib.request


CORE_API_URL = "https://core-api.internal/v1/notifications/send"


def send_notification(user_id: str, template: str) -> dict:
    body = {"user_id": user_id, "template": template}
    request = urllib.request.Request(
        CORE_API_URL,
        method="POST",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=5) as handle:
        return json.loads(handle.read())
