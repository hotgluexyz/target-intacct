"""Tests for open bank reconciliation helpers."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

from target_intacct.bank_reconciliation import (
    create_bank_reconciliation,
    ensure_open_bank_reconciliation,
    find_open_bank_reconciliation,
)


def test_find_open_bank_reconciliation_filters_state_and_mode() -> None:
    client = MagicMock()
    client.request_api.return_value = {
        "data": {
            "BANKACCTRECON": [
                {
                    "RECORDNO": "1",
                    "FINANCIALENTITY": "300_SVB",
                    "STATE": "reconciled",
                    "MODE": "AutomatchReview",
                    "FEEDTYPE": "xml",
                },
                {
                    "RECORDNO": "6",
                    "FINANCIALENTITY": "300_SVB",
                    "STATE": "initiated",
                    "MODE": "AutomatchReview",
                    "FEEDTYPE": "xml",
                },
            ]
        }
    }

    found = find_open_bank_reconciliation(client, financial_entity="300_SVB")
    assert found is not None
    assert found["RECORDNO"] == "6"


def test_ensure_reuses_existing_open_recon() -> None:
    client = MagicMock()
    client.location_id = "100"
    client.request_api.return_value = {
        "data": {
            "BANKACCTRECON": {
                "RECORDNO": "6",
                "FINANCIALENTITY": "300_SVB",
                "STATE": "initiated",
                "MODE": "AutomatchReview",
                "FEEDTYPE": "xml",
            }
        }
    }
    cache: dict[str, str] = {}

    recon_id = ensure_open_bank_reconciliation(
        client,
        financial_entity="300_SVB",
        feed_date="09/23/2026",
        logger=logging.getLogger("test"),
        cache=cache,
    )

    assert recon_id == "6"
    assert cache["300_SVB|AutomatchReview|xml"] == "6"
    assert client.location_id == "100"
    # Switched to top-level for the query, then restored.
    assert client.location_id == "100"


def test_ensure_creates_recon_when_missing() -> None:
    client = MagicMock()
    client.location_id = "100"
    client.request_api.side_effect = [
        {"data": {"BANKACCTRECON": []}},
        {"data": {"bankacctrecon": {"RECORDNO": "9"}}},
    ]
    cache: dict[str, str] = {}

    recon_id = ensure_open_bank_reconciliation(
        client,
        financial_entity="300_SVB",
        feed_date="09/23/2026",
        logger=logging.getLogger("test"),
        cache=cache,
    )

    assert recon_id == "9"
    create_call = client.request_api.call_args_list[1].args[0]
    assert create_call == {
        "create": {
            "BANKACCTRECON": {
                "FINANCIALENTITY": "300_SVB",
                "STMTENDINGDATE": "09/30/2026",
                "STMTENDINGBALANCE": "0",
                "MODE": "AutomatchReview",
                "FEEDTYPE": "xml",
                "CUTOFFDATE": "09/01/2026",
            }
        }
    }


def test_create_bank_reconciliation_defaults() -> None:
    client = MagicMock()
    client.request_api.return_value = {"key": "12"}

    recon_id = create_bank_reconciliation(
        client,
        financial_entity="300_SVB",
        feed_date="09/22/2026",
        logger=logging.getLogger("test"),
    )

    assert recon_id == "12"
    payload = client.request_api.call_args.args[0]["create"]["BANKACCTRECON"]
    assert payload["STMTENDINGDATE"] == "09/30/2026"
    assert payload["CUTOFFDATE"] == "09/01/2026"
    assert payload["STMTENDINGBALANCE"] == "0"
