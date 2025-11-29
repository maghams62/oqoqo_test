#!/usr/bin/env python3
"""
Create the synthetic multi-repo Git dataset described in the Oqoqo brief and
optionally push the generated artifacts to a branch/remote configured via
environment variables or `config.yaml`.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, Iterable, List, Optional, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils import load_config  # noqa: E402


# ---------------------------------------------------------------------------
# Data model helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FileSpec:
    path: str
    content: str


@dataclass(frozen=True)
class CommitSpec:
    message: str
    summary: str
    author: str
    author_email: str
    timestamp: str  # ISO 8601 UTC string
    files: Sequence[FileSpec]
    service_ids: Sequence[str]
    component_ids: Sequence[str]
    changed_apis: Sequence[str] = field(default_factory=tuple)
    is_doc_change: bool = False


@dataclass(frozen=True)
class RepoPlan:
    name: str
    repo_url: str
    default_branch: str
    commits: Sequence[CommitSpec]


@dataclass(frozen=True)
class PrTemplate:
    repo: str
    repo_url: str
    branch: str
    pr_number: int
    author: str
    timestamp: str
    title: str
    body: str
    merged: bool
    text_for_embedding: str
    service_ids: Sequence[str]
    component_ids: Sequence[str]
    changed_apis: Sequence[str]
    labels: Sequence[str]
    commit_messages: Sequence[str]


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------


def run_git(args: Sequence[str], *, cwd: Path, capture: bool = False, env: Optional[Dict[str, str]] = None) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        check=True,
        capture_output=capture,
        text=True,
        env=env,
    )
    if capture:
        return result.stdout.strip()
    return ""


def ensure_branch_checked_out(branch: str, base_branch: str) -> None:
    """Ensure repo root is on the requested branch, creating it off base_branch if needed."""
    current = run_git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=PROJECT_ROOT, capture=True)
    if current == branch:
        return

    existing = run_git(["branch", "--list", branch], cwd=PROJECT_ROOT, capture=True)
    if existing:
        run_git(["switch", branch], cwd=PROJECT_ROOT)
        return

    if base_branch and base_branch != branch:
        run_git(["switch", base_branch], cwd=PROJECT_ROOT)

    try:
        run_git(["switch", "-c", branch], cwd=PROJECT_ROOT)
    except subprocess.CalledProcessError:
        # Fallback for older Git versions
        run_git(["checkout", "-b", branch], cwd=PROJECT_ROOT)


# ---------------------------------------------------------------------------
# Synthetic repo content
# ---------------------------------------------------------------------------


def payments_py_initial() -> str:
    return dedent(
        """\
        \"\"\"Core payments workflow.\"\"\"


        def create_payment(amount: float, currency: str) -> dict:
            \"\"\"Create a payment using the initial contract (no VAT).\"\"\"
            if amount <= 0:
                raise ValueError("amount must be positive")
            return {
                "amount": amount,
                "currency": currency.upper(),
                "status": "pending",
                "requires_vat_code": False,
            }
        """
    )


def payments_py_optional_vat() -> str:
    return dedent(
        """\
        \"\"\"Core payments workflow.\"\"\"


        def create_payment(amount: float, currency: str, vat_code: str | None = None) -> dict:
            \"\"\"Allow downstream callers to include a VAT code (optional).\"\"\"
            if amount <= 0:
                raise ValueError("amount must be positive")
            payload = {
                "amount": amount,
                "currency": currency.upper(),
                "status": "pending",
                "requires_vat_code": False,
            }
            if vat_code:
                payload["vat_code"] = vat_code
            return payload
        """
    )


def payments_py_required_vat() -> str:
    return dedent(
        """\
        \"\"\"Core payments workflow with VAT enforcement for EU regions.\"\"\"


        EU_CURRENCIES = {"EUR", "SEK", "DKK", "NOK"}


        def create_payment(
            amount: float,
            currency: str,
            *,
            region: str = "US",
            vat_code: str | None = None,
        ) -> dict:
            \"\"\"Require vat_code for EU customers to satisfy compliance.\"\"\"
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
        """
    )


def openapi_payments_initial() -> str:
    return dedent(
        """\
        openapi: 3.0.0
        info:
          title: Core Payments API
          version: "1.0.0"
        paths:
          /v1/payments/create:
            post:
              summary: Create a payment
              requestBody:
                required: true
                content:
                  application/json:
                    schema:
                      type: object
                      required: [amount, currency]
                      properties:
                        amount:
                          type: number
                        currency:
                          type: string
              responses:
                "200":
                  description: Payment created
        """
    )


def openapi_payments_optional_vat() -> str:
    return dedent(
        """\
        openapi: 3.0.0
        info:
          title: Core Payments API
          version: "1.1.0"
        paths:
          /v1/payments/create:
            post:
              summary: Create a payment (VAT optional)
              requestBody:
                required: true
                content:
                  application/json:
                    schema:
                      type: object
                      required: [amount, currency]
                      properties:
                        amount:
                          type: number
                        currency:
                          type: string
                        vat_code:
                          type: string
                          description: Optional VAT code for EU merchants
              responses:
                "200":
                  description: Payment created
        """
    )


def openapi_payments_required_vat() -> str:
    return dedent(
        """\
        openapi: 3.0.0
        info:
          title: Core Payments API
          version: "2.0.0"
        paths:
          /v1/payments/create:
            post:
              summary: Create a payment (VAT required for EU)
              requestBody:
                required: true
                content:
                  application/json:
                    schema:
                      type: object
                      required: [amount, currency, region]
                      properties:
                        amount:
                          type: number
                        currency:
                          type: string
                        region:
                          type: string
                          enum: [US, EU, ROW]
                        vat_code:
                          type: string
                          description: Required when region is EU
              responses:
                "200":
                  description: Payment created
        """
    )


def core_api_docs() -> str:
    return dedent(
        """\
        # Payments API

        The `/v1/payments/create` endpoint submits a new payment using the shared
        library. Initial integration only requires `amount` and `currency`.

        Downstream teams embed this module directly or call over HTTP.
        """
    )


def billing_checkout_initial() -> str:
    return dedent(
        """\
        from .core_api_client import create_payment


        def checkout(cart):
            payload = create_payment(amount=cart.total, currency=cart.currency)
            if payload["status"] != "pending":
                raise RuntimeError("Unexpected payment state")
            return {"status": "ok", "payment": payload}
        """
    )


def billing_core_api_client_initial() -> str:
    return dedent(
        """\
        import json
        import urllib.request


        CORE_API_URL = "https://core-api.internal/v1/payments/create"


        def create_payment(*, amount: float, currency: str) -> dict:
            request = urllib.request.Request(
                CORE_API_URL,
                method="POST",
                data=json.dumps({"amount": amount, "currency": currency}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(request, timeout=5) as handle:
                return json.loads(handle.read())
        """
    )


def billing_checkout_expanded() -> str:
    return dedent(
        """\
        from .core_api_client import create_payment


        def checkout(cart):
            payload = create_payment(amount=cart.total_with_discounts(), currency=cart.currency)
            if payload["status"] != "pending":
                raise RuntimeError("Unexpected payment state")

            if cart.requires_invoice():
                payload["invoice_id"] = cart.invoice_id

            return {"status": "ok", "payment": payload}
        """
    )


def billing_core_api_with_vat() -> str:
    return dedent(
        """\
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
        """
    )


def billing_checkout_partial_fix() -> str:
    return dedent(
        """\
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
        """
    )


def billing_docs_onboarding() -> str:
    return dedent(
        """\
        # Billing Onboarding

        1. Collect the cart total.
        2. Call `/v1/payments/create` with `amount` and `currency`.
        3. Record the `payment_id` for future reconciliation.

        VAT codes are *not* required for the current contract.
        """
    )


def billing_docs_api_usage(updated: bool) -> str:
    if updated:
        extra = (
            "\n- EU carts must attach a VAT code after the core-api 2.0 breaking change."
        )
    else:
        extra = ""
    return dedent(
        f"""\
        # API Usage

        Billing depends on the shared core-api. Requests currently mirror the OpenAPI spec.
        {extra}
        """
    )


def billing_config() -> str:
    return dedent(
        """\
        core_api:
          url: https://core-api.internal/v1/payments/create
          timeout_seconds: 5
        """
    )


def notifications_py() -> str:
    return dedent(
        """\
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
        """
    )


def scheduler_py(interval: int) -> str:
    return dedent(
        f"""\
        import time

        DEFAULT_INTERVAL_SECONDS = {interval}


        def run_scheduler(dispatch_fn):
            while True:
                dispatch_fn()
                time.sleep(DEFAULT_INTERVAL_SECONDS)
        """
    )


def notification_doc() -> str:
    return dedent(
        """\
        # Notification Playbook

        The service calls `/v1/notifications/send` to deliver receipts after billing events.
        """
    )


def docs_portal_payments(initial: bool) -> str:
    if initial:
        note = "VAT is not part of the request yet."
    else:
        note = "VAT is required for EU customers but the billing flow doc still needs an update."
    return dedent(
        f"""\
        # Payments API (Docs Portal)

        - Endpoint: `POST /v1/payments/create`
        - Required fields: `amount`, `currency`
        - Notes: {note}
        """
    )


def docs_portal_billing(initial: bool) -> str:
    detail = "amount and currency" if initial else "amount, currency, vat_code (missing!)"
    return dedent(
        f"""\
        # Billing Flows

        The billing team still references older payloads containing {detail}.
        """
    )


def docs_portal_changelog(entries: Iterable[str]) -> str:
    return "# Changelog\n\n" + "\n".join(f"- {line}" for line in entries) + "\n"


# ---------------------------------------------------------------------------
# Dataset plan construction
# ---------------------------------------------------------------------------


def build_repo_plans() -> List[RepoPlan]:
    base_author = ("alice", "alice@oqoqo.local")
    repo_plans = [
        RepoPlan(
            name="core-api",
            repo_url="https://github.com/acme/core-api",
            default_branch="main",
            commits=[
                CommitSpec(
                    message="feat: add payments endpoint",
                    summary="Bootstrap the shared payments module and OpenAPI spec.",
                    author=base_author[0],
                    author_email=base_author[1],
                    timestamp="2025-11-24T09:00:00Z",
                    files=[
                        FileSpec("README.md", "# Core API\n\nShared payments/auth helpers.\n"),
                        FileSpec("src/auth.py", dedent("def authenticate(token: str) -> bool:\n    return token.startswith(\"tok_\")\n")),
                        FileSpec("src/payments.py", payments_py_initial()),
                        FileSpec("openapi/payments.yaml", openapi_payments_initial()),
                        FileSpec("docs/payments.md", core_api_docs()),
                    ],
                    service_ids=["core-api-service"],
                    component_ids=["core.payments"],
                    changed_apis=["/v1/payments/create"],
                ),
                CommitSpec(
                    message="feat: allow optional vat_code",
                    summary="Allow downstream callers to pass vat_code without enforcing it.",
                    author=base_author[0],
                    author_email=base_author[1],
                    timestamp="2025-11-24T16:00:00Z",
                    files=[
                        FileSpec("src/payments.py", payments_py_optional_vat()),
                        FileSpec("openapi/payments.yaml", openapi_payments_optional_vat()),
                    ],
                    service_ids=["core-api-service"],
                    component_ids=["core.payments"],
                    changed_apis=["/v1/payments/create"],
                ),
                CommitSpec(
                    message="feat!: require vat_code for EU",
                    summary="Breaking change: vat_code must be provided for EU payments.",
                    author=base_author[0],
                    author_email=base_author[1],
                    timestamp="2025-11-25T10:15:00Z",
                    files=[
                        FileSpec("src/payments.py", payments_py_required_vat()),
                        FileSpec("openapi/payments.yaml", openapi_payments_required_vat()),
                    ],
                    service_ids=["core-api-service"],
                    component_ids=["core.payments"],
                    changed_apis=["/v1/payments/create"],
                ),
            ],
        ),
        RepoPlan(
            name="billing-service",
            repo_url="https://github.com/acme/billing-service",
            default_branch="main",
            commits=[
                CommitSpec(
                    message="feat: initial checkout integration",
                    summary="Initial billing->core-api wiring without vat_code.",
                    author="bob",
                    author_email="bob@oqoqo.local",
                    timestamp="2025-11-24T11:30:00Z",
                    files=[
                        FileSpec("README.md", "# Billing Service\n\nHandles checkout orchestration.\n"),
                        FileSpec("src/core_api_client.py", billing_core_api_client_initial()),
                        FileSpec("src/checkout.py", billing_checkout_initial()),
                        FileSpec("config/core_api.yml", billing_config()),
                        FileSpec("docs/billing_onboarding.md", billing_docs_onboarding()),
                        FileSpec("docs/api_usage.md", billing_docs_api_usage(False)),
                    ],
                    service_ids=["billing-service"],
                    component_ids=["billing.checkout"],
                    changed_apis=["/v1/payments/create"],
                ),
                CommitSpec(
                    message="feat: expand checkout rules",
                    summary="Add invoice path but still rely on old API contract.",
                    author="bob",
                    author_email="bob@oqoqo.local",
                    timestamp="2025-11-24T18:45:00Z",
                    files=[
                        FileSpec("src/checkout.py", billing_checkout_expanded()),
                    ],
                    service_ids=["billing-service"],
                    component_ids=["billing.checkout"],
                    changed_apis=["/v1/payments/create"],
                ),
                CommitSpec(
                    message="fix: include vat_code for EU carts",
                    summary="Patch checkout to pass vat_code but only for invoice flows.",
                    author="bob",
                    author_email="bob@oqoqo.local",
                    timestamp="2025-11-26T09:10:00Z",
                    files=[
                        FileSpec("src/core_api_client.py", billing_core_api_with_vat()),
                        FileSpec("src/checkout.py", billing_checkout_partial_fix()),
                    ],
                    service_ids=["billing-service"],
                    component_ids=["billing.checkout"],
                    changed_apis=["/v1/payments/create"],
                ),
                CommitSpec(
                    message="docs: refresh onboarding",
                    summary="Update API usage notes but onboarding doc still omits vat_code.",
                    author="carol",
                    author_email="carol@oqoqo.local",
                    timestamp="2025-11-26T16:00:00Z",
                    files=[
                        FileSpec("docs/api_usage.md", billing_docs_api_usage(True)),
                    ],
                    service_ids=["billing-service"],
                    component_ids=["billing.checkout"],
                    changed_apis=["/v1/payments/create"],
                    is_doc_change=True,
                ),
            ],
        ),
        RepoPlan(
            name="notifications-service",
            repo_url="https://github.com/acme/notifications-service",
            default_branch="main",
            commits=[
                CommitSpec(
                    message="feat: send payment receipt notifications",
                    summary="Wire notifications to call /v1/notifications/send.",
                    author="dave",
                    author_email="dave@oqoqo.local",
                    timestamp="2025-11-24T12:15:00Z",
                    files=[
                        FileSpec("README.md", "# Notifications Service\n\nSends receipts post-payment.\n"),
                        FileSpec("src/notifications.py", notifications_py()),
                        FileSpec("src/scheduler.py", scheduler_py(60)),
                        FileSpec("docs/notification_playbook.md", notification_doc()),
                    ],
                    service_ids=["notifications-service"],
                    component_ids=["notifications.dispatch"],
                    changed_apis=["/v1/notifications/send"],
                ),
                CommitSpec(
                    message="chore: tweak scheduler interval",
                    summary="Increase polling interval to reduce load.",
                    author="dave",
                    author_email="dave@oqoqo.local",
                    timestamp="2025-11-24T18:00:00Z",
                    files=[
                        FileSpec("src/scheduler.py", scheduler_py(120)),
                    ],
                    service_ids=["notifications-service"],
                    component_ids=["notifications.dispatch"],
                    changed_apis=[],
                ),
            ],
        ),
        RepoPlan(
            name="docs-portal",
            repo_url="https://github.com/acme/docs-portal",
            default_branch="main",
            commits=[
                CommitSpec(
                    message="docs: document payments API",
                    summary="Initial docs portal entry without vat_code.",
                    author="eve",
                    author_email="eve@oqoqo.local",
                    timestamp="2025-11-24T13:00:00Z",
                    files=[
                        FileSpec("docs/payments_api.md", docs_portal_payments(True)),
                        FileSpec("docs/billing_flows.md", docs_portal_billing(True)),
                        FileSpec("docs/changelog.md", docs_portal_changelog(["Initial payments docs published."])),
                    ],
                    service_ids=["docs-portal"],
                    component_ids=["docs.payments"],
                    changed_apis=["/v1/payments/create"],
                    is_doc_change=True,
                ),
                CommitSpec(
                    message="docs: partial VAT update",
                    summary="Payments doc references VAT but billing flows remain stale.",
                    author="eve",
                    author_email="eve@oqoqo.local",
                    timestamp="2025-11-26T11:00:00Z",
                    files=[
                        FileSpec("docs/payments_api.md", docs_portal_payments(False)),
                        FileSpec("docs/changelog.md", docs_portal_changelog([
                            "Initial payments docs published.",
                            "Added VAT note (billing flow pending update).",
                        ])),
                    ],
                    service_ids=["docs-portal"],
                    component_ids=["docs.payments"],
                    changed_apis=["/v1/payments/create"],
                    is_doc_change=True,
                ),
            ],
        ),
    ]
    return repo_plans


def build_pr_templates() -> List[PrTemplate]:
    return [
        PrTemplate(
            repo="core-api",
            repo_url="https://github.com/acme/core-api",
            branch="main",
            pr_number=2041,
            author="alice",
            timestamp="2025-11-25T14:00:00Z",
            title="Add required vat_code to /v1/payments/create",
            body=(
                "Breaking change: /v1/payments/create now requires vat_code for EU customers. "
                "Downstream services must update their integrations and doc owners should refresh guides."
            ),
            merged=True,
            text_for_embedding=(
                "PR #2041: Add required vat_code to /v1/payments/create. "
                "Breaking change requiring vat_code for EU customers."
            ),
            service_ids=["core-api-service"],
            component_ids=["core.payments"],
            changed_apis=["/v1/payments/create"],
            labels=["breaking_change", "api_contract"],
            commit_messages=["feat!: require vat_code for EU"],
        ),
        PrTemplate(
            repo="billing-service",
            repo_url="https://github.com/acme/billing-service",
            branch="main",
            pr_number=118,
            author="bob",
            timestamp="2025-11-26T12:30:00Z",
            title="Fix 400 errors by adding vat_code to payments API calls",
            body=(
                "We are seeing 400s from core-api after vat_code became mandatory. "
                "This PR threads vat_code through checkout and notes that docs still need a deeper refresh."
            ),
            merged=True,
            text_for_embedding=(
                "PR #118: Fix 400 errors by adding vat_code to payments API calls. "
                "Adds vat_code payloads for EU carts but onboarding doc is still incomplete."
            ),
            service_ids=["billing-service"],
            component_ids=["billing.checkout"],
            changed_apis=["/v1/payments/create"],
            labels=["incident", "docs_followup"],
            commit_messages=["fix: include vat_code for EU carts", "docs: refresh onboarding"],
        ),
    ]


# ---------------------------------------------------------------------------
# Generation + export
# ---------------------------------------------------------------------------


def initialize_repo(repo_dir: Path, default_branch: str) -> None:
    repo_dir.mkdir(parents=True, exist_ok=True)
    run_git(["init", "-b", default_branch], cwd=repo_dir)
    run_git(["config", "user.name", "Synthetic Git Bot"], cwd=repo_dir)
    run_git(["config", "user.email", "synthetic@example.com"], cwd=repo_dir)


def write_commit(repo_dir: Path, plan: RepoPlan, spec: CommitSpec) -> Dict[str, Any]:
    paths = []
    for file_spec in spec.files:
        file_path = repo_dir / file_spec.path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(file_spec.content, encoding="utf-8")
        paths.append(file_spec.path)

    run_git(["add", *paths], cwd=repo_dir)

    env = os.environ.copy()
    env.update(
        {
            "GIT_AUTHOR_NAME": spec.author,
            "GIT_AUTHOR_EMAIL": spec.author_email,
            "GIT_COMMITTER_NAME": spec.author,
            "GIT_COMMITTER_EMAIL": spec.author_email,
            "GIT_AUTHOR_DATE": spec.timestamp,
            "GIT_COMMITTER_DATE": spec.timestamp,
        }
    )
    run_git(["commit", "-m", spec.message], cwd=repo_dir, env=env)
    sha = run_git(["rev-parse", "HEAD"], cwd=repo_dir, capture=True)

    record = {
        "id": f"git_commit:{plan.name}:{sha}",
        "source_type": "git_commit",
        "repo": plan.name,
        "repo_url": plan.repo_url,
        "branch": plan.default_branch,
        "commit_sha": sha,
        "author": spec.author,
        "timestamp": spec.timestamp,
        "message": spec.message,
        "files_changed": list(paths),
        "text_for_embedding": f"{spec.message}\n\n{spec.summary}",
        "service_ids": list(spec.service_ids),
        "component_ids": list(spec.component_ids),
        "changed_apis": list(spec.changed_apis),
        "is_doc_change": spec.is_doc_change,
    }
    return record


def generate_dataset(base_dir: Path, *, force: bool) -> Dict[str, Any]:
    if base_dir.exists():
        if force:
            shutil.rmtree(base_dir)
        else:
            raise SystemExit(f"{base_dir} already exists. Re-run with --force to overwrite.")

    base_dir.mkdir(parents=True, exist_ok=True)

    repo_plans = build_repo_plans()
    commit_records: List[Dict[str, Any]] = []
    commit_lookup: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for plan in repo_plans:
        repo_path = base_dir / plan.name
        initialize_repo(repo_path, plan.default_branch)
        repo_commits: Dict[str, Dict[str, Any]] = {}

        for spec in plan.commits:
            record = write_commit(repo_path, plan, spec)
            commit_records.append(record)
            repo_commits[spec.message] = record

        commit_lookup[plan.name] = repo_commits

    events_path = base_dir / "git_events.json"
    events_path.write_text(json.dumps(commit_records, indent=2) + "\n", encoding="utf-8")

    return {
        "commit_lookup": commit_lookup,
        "events_path": events_path,
    }


def build_pr_dataset(pr_templates: Sequence[PrTemplate], commit_lookup: Dict[str, Dict[str, Dict[str, Any]]]) -> List[Dict[str, Any]]:
    pr_records: List[Dict[str, Any]] = []
    for template in pr_templates:
        repo_commits = commit_lookup.get(template.repo)
        if not repo_commits:
            continue

        files: List[str] = []
        for message in template.commit_messages:
            commit_record = repo_commits.get(message)
            if not commit_record:
                continue
            files.extend(commit_record["files_changed"])

        seen = []
        for path in files:
            if path not in seen:
                seen.append(path)

        pr_records.append(
            {
                "id": f"git_pr:{template.repo}:{template.pr_number}",
                "source_type": "git_pr",
                "repo": template.repo,
                "repo_url": template.repo_url,
                "branch": template.branch,
                "pr_number": template.pr_number,
                "author": template.author,
                "timestamp": template.timestamp,
                "title": template.title,
                "body": template.body,
                "merged": template.merged,
                "files_changed": seen,
                "text_for_embedding": template.text_for_embedding,
                "service_ids": list(template.service_ids),
                "component_ids": list(template.component_ids),
                "changed_apis": list(template.changed_apis),
                "labels": list(template.labels),
            }
        )
    return pr_records


def export_prs(base_dir: Path, pr_templates: Sequence[PrTemplate], commit_lookup: Dict[str, Dict[str, Dict[str, Any]]]) -> Path:
    pr_records = build_pr_dataset(pr_templates, commit_lookup)
    prs_path = base_dir / "git_prs.json"
    prs_path.write_text(json.dumps(pr_records, indent=2) + "\n", encoding="utf-8")
    return prs_path


# ---------------------------------------------------------------------------
# Push helpers
# ---------------------------------------------------------------------------


def resolve_branch(args_branch: Optional[str], config: Dict[str, Any]) -> str:
    if args_branch:
        return args_branch
    env_vars = ["SYNTHETIC_GIT_BRANCH", "GIT_DATA_BRANCH"]
    for var in env_vars:
        val = os.getenv(var)
        if val:
            return val
    synth_cfg = config.get("synthetic_git", {})
    if synth_cfg.get("branch"):
        return synth_cfg["branch"]
    github_cfg = config.get("github", {})
    return github_cfg.get("base_branch", "main")


def remote_from_token(config: Dict[str, Any]) -> str:
    token = os.getenv("SYNTHETIC_GIT_TOKEN") or os.getenv("GITHUB_TOKEN")
    owner = (
        os.getenv("SYNTHETIC_GIT_REPO_OWNER")
        or (config.get("synthetic_git", {}) or {}).get("repo_owner")
        or os.getenv("GITHUB_REPO_OWNER")
        or (config.get("github", {}) or {}).get("repo_owner")
    )
    repo_name = (
        os.getenv("SYNTHETIC_GIT_REPO_NAME")
        or (config.get("synthetic_git", {}) or {}).get("repo_name")
        or os.getenv("GITHUB_REPO_NAME")
        or (config.get("github", {}) or {}).get("repo_name")
    )
    if token and owner and repo_name:
        return f"https://{token}@github.com/{owner}/{repo_name}.git"
    return ""


def resolve_remote(args_remote: Optional[str], config: Dict[str, Any]) -> str:
    if args_remote:
        return args_remote
    env_vars = ["SYNTHETIC_GIT_REMOTE", "SYNTHETIC_GIT_REMOTE_URL", "GIT_DATA_REMOTE"]
    for var in env_vars:
        val = os.getenv(var)
        if val:
            return val
    synth_cfg = config.get("synthetic_git", {})
    if synth_cfg.get("remote_url"):
        return synth_cfg["remote_url"]
    token_remote = remote_from_token(config)
    if token_remote:
        return token_remote
    return "origin"


def mask_remote(remote: str) -> str:
    if "@" not in remote or "://" not in remote:
        return remote
    scheme, rest = remote.split("://", 1)
    if "@" not in rest:
        return remote
    _, suffix = rest.split("@", 1)
    return f"{scheme}://***@{suffix}"


def stage_and_commit(base_dir: Path, include_all: bool, commit_message: str) -> bool:
    target = "."
    if not include_all:
        try:
            rel_path = base_dir.relative_to(PROJECT_ROOT)
        except ValueError:
            raise SystemExit(f"{base_dir} is outside the repository root; cannot stage selectively.")
        target = rel_path.as_posix()

    run_git(["add", "-A", target], cwd=PROJECT_ROOT)
    diff_result = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        cwd=str(PROJECT_ROOT),
        check=False,
    )
    if diff_result.returncode == 0:
        return False
    run_git(["commit", "-m", commit_message], cwd=PROJECT_ROOT)
    return True


def push_dataset(remote: str, branch: str) -> None:
    run_git(["push", "--set-upstream", remote, branch], cwd=PROJECT_ROOT)


def synthetic_commit_message(config: Dict[str, Any]) -> str:
    synth_cfg = config.get("synthetic_git", {})
    base = synth_cfg.get("commit_message", "chore(synthetic-git): refresh dataset")
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"{base} @ {timestamp}"


def base_dir_from_args(args_base: Optional[str], config: Dict[str, Any]) -> Path:
    synth_cfg = config.get("synthetic_git", {})
    configured = synth_cfg.get("base_dir") if synth_cfg else None
    raw = args_base or configured or "data/synthetic_git"
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def base_branch_for_push(config: Dict[str, Any]) -> str:
    synth_cfg = config.get("synthetic_git", {})
    if synth_cfg.get("base_branch"):
        return synth_cfg["base_branch"]
    github_cfg = config.get("github", {})
    return github_cfg.get("base_branch", "main")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate and optionally push the synthetic Git dataset.")
    parser.add_argument("--base-dir", help="Directory to create the synthetic repos in (default: config synthetic_git.base_dir).")
    parser.add_argument("--force", action="store_true", help="Overwrite base directory if it already exists.")
    parser.add_argument("--skip-generate", action="store_true", help="Skip regeneration and only push existing artifacts.")
    parser.add_argument("--push", action="store_true", help="Commit/push dataset changes after generation.")
    parser.add_argument("--branch", help="Target branch override for pushing.")
    parser.add_argument("--remote", help="Remote name or URL override for pushing.")
    parser.add_argument("--include-all", action="store_true", help="Stage the entire repo instead of just the synthetic dataset path.")
    args = parser.parse_args()

    config = load_config()
    base_dir = base_dir_from_args(args.base_dir, config)
    branch = resolve_branch(args.branch, config)
    base_branch = base_branch_for_push(config)

    if args.push:
        ensure_branch_checked_out(branch, base_branch)

    if not args.skip_generate:
        result = generate_dataset(base_dir, force=args.force)
        prs_path = export_prs(base_dir, build_pr_templates(), result["commit_lookup"])
        print(f"Generated synthetic repos under {base_dir}")
        print(f"  - git_events.json: {result['events_path']}")
        print(f"  - git_prs.json: {prs_path}")
    else:
        print("Skipping generation step; assuming artifacts already exist.")

    if args.push:
        remote = resolve_remote(args.remote, config)
        commit_message = synthetic_commit_message(config)
        changed = stage_and_commit(base_dir, args.include_all, commit_message)
        if not changed:
            print("No staged changes to commit; skipping push.")
            return
        safe_remote = mask_remote(remote)
        print(f"Pushing {base_dir} to {safe_remote} ({branch})...")
        push_dataset(remote, branch)
        print("Push complete.")


if __name__ == "__main__":
    main()
