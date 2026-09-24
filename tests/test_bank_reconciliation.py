"""Tests for open bank reconciliation helpers."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest
from hotglue_singer_sdk.exceptions import FatalAPIError

from target_intacct.bank_reconciliation import (
    create_bank_reconciliation,
    ensure_open_bank_reconciliation,
    find_open_bank_reconciliation,
    resolve_session_location_for_account,
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


def test_resolve_session_uses_mega_entity() -> None:
    client = MagicMock()
    client.location_id = "100"
    client.request_api.return_value = {
        "data": {
            "checkingaccount": {
                "BANKACCOUNTID": "60b66f6ab5f211f1a999",
                "LOCATIONID": "110",
                "RECORDNO": "15",
                "MEGAENTITYID": "100",
            }
        }
    }

    assert (
        resolve_session_location_for_account(
            client,
            financial_entity="60b66f6ab5f211f1a999",
            logger=logging.getLogger("test"),
        )
        == "100"
    )


def test_resolve_session_top_level_when_unrestricted() -> None:
    client = MagicMock()
    client.location_id = "100"
    client.request_api.return_value = {
        "data": {
            "checkingaccount": {
                "BANKACCOUNTID": "300_SVB",
                "LOCATIONID": "300",
                "RECORDNO": "3",
                "MEGAENTITYID": None,
            }
        }
    }

    assert (
        resolve_session_location_for_account(
            client,
            financial_entity="300_SVB",
            logger=logging.getLogger("test"),
        )
        is None
    )


def test_ensure_sets_mega_entity_and_reuses_open_recon() -> None:
    client = MagicMock()
    client.location_id = None
    client.request_api.side_effect = [
        {
            "data": {
                "checkingaccount": {
                    "BANKACCOUNTID": "60b66f6ab5f211f1a999",
                    "LOCATIONID": "110",
                    "RECORDNO": "15",
                    "MEGAENTITYID": "100",
                    "RULESETKEY": "3",
                    "RULESETID": "HG-MATCH",
                }
            }
        },
        {
            "data": {
                "BANKACCTRECON": {
                    "RECORDNO": "6",
                    "FINANCIALENTITY": "60b66f6ab5f211f1a999",
                    "STATE": "initiated",
                    "MODE": "AutomatchReview",
                    "FEEDTYPE": "xml",
                }
            }
        },
    ]
    cache: dict[str, str] = {}

    recon_id = ensure_open_bank_reconciliation(
        client,
        financial_entity="60b66f6ab5f211f1a999",
        feed_date="09/23/2026",
        logger=logging.getLogger("test"),
        cache=cache,
    )

    assert recon_id == "6"
    assert client.location_id == "100"
    assert cache["60b66f6ab5f211f1a999|AutomatchReview|xml"] == "6"


def test_ensure_creates_recon_when_missing() -> None:
    client = MagicMock()
    client.location_id = "100"
    client.request_api.side_effect = [
        {
            "data": {
                "checkingaccount": {
                    "BANKACCOUNTID": "300_SVB",
                    "LOCATIONID": "300",
                    "RECORDNO": "3",
                    "MEGAENTITYID": None,
                    "RULESETKEY": "1",
                    "RULESETID": "001",
                }
            }
        },
        {"data": {"BANKACCTRECON": []}},
        {"data": {"bankacctrecon": {"RECORDNO": "9"}}},
        {
            "data": {
                "BANKACCTRECON": {
                    "RECORDNO": "9",
                    "MODE": "AutomatchReview",
                    "FEEDTYPE": "xml",
                    "STATE": "initiated",
                }
            }
        },
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
    assert client.location_id is None
    create_call = client.request_api.call_args_list[2].args[0]
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


def test_ensure_assigns_entity_match_ruleset_when_missing() -> None:
    from target_intacct.bank_reconciliation import ensure_checking_account_rule_set

    client = MagicMock()
    client.location_id = None
    client.request_api.side_effect = [
        {
            "data": {
                "checkingaccount": {
                    "BANKACCOUNTID": "acct1",
                    "LOCATIONID": "110",
                    "RECORDNO": "15",
                    "MEGAENTITYID": "100",
                    "RULESETKEY": None,
                }
            }
        },
        # find HG-MATCH — missing
        {"data": {"banktxnruleset": None}},
        # create HG-MATCH
        {"data": {"banktxnruleset": {"RECORDNO": "3"}}},
        # read ruleset maps — empty
        {"data": {"BANKTXNRULESET": {"RECORDNO": "3", "BANKTXNRULEMAPS": {}}}},
        # map match rule
        {"data": {}},
        # assign to checking account
        {"data": {}},
    ]

    key = ensure_checking_account_rule_set(
        client,
        financial_entity="acct1",
        logger=logging.getLogger("test"),
    )

    assert key == "3"
    update = client.request_api.call_args_list[-1].args[0]
    assert update == {
        "update": {
            "CHECKINGACCOUNT": {
                "BANKACCOUNTID": "acct1",
                "RULESETKEY": "3",
            }
        }
    }


def test_ensure_assigns_top_level_ruleset_for_unrestricted() -> None:
    from target_intacct.bank_reconciliation import ensure_checking_account_rule_set

    client = MagicMock()
    client.location_id = "100"
    client.request_api.side_effect = [
        {
            "data": {
                "checkingaccount": {
                    "BANKACCOUNTID": "300_SVB",
                    "LOCATIONID": "300",
                    "RECORDNO": "3",
                    "MEGAENTITYID": None,
                    "RULESETKEY": None,
                }
            }
        },
        {"data": {}},
    ]

    key = ensure_checking_account_rule_set(
        client,
        financial_entity="300_SVB",
        logger=logging.getLogger("test"),
    )

    assert key == "1"
    update = client.request_api.call_args_list[-1].args[0]
    assert update["update"]["CHECKINGACCOUNT"]["RULESETKEY"] == "1"


def test_create_bank_reconciliation_defaults() -> None:
    client = MagicMock()
    client.location_id = None
    client.request_api.side_effect = [
        {"key": "12"},
        {
            "data": {
                "BANKACCTRECON": {
                    "RECORDNO": "12",
                    "MODE": "AutomatchReview",
                    "FEEDTYPE": "xml",
                    "STATE": "initiated",
                }
            }
        },
    ]

    recon_id = create_bank_reconciliation(
        client,
        financial_entity="300_SVB",
        feed_date="09/22/2026",
        logger=logging.getLogger("test"),
    )

    assert recon_id == "12"
    payload = client.request_api.call_args_list[0].args[0]["create"]["BANKACCTRECON"]
    assert payload["STMTENDINGDATE"] == "09/30/2026"
    assert payload["CUTOFFDATE"] == "09/01/2026"
    assert payload["STMTENDINGBALANCE"] == "0"


def test_create_bank_reconciliation_rejects_silent_manual_mode() -> None:
    client = MagicMock()
    client.location_id = None
    client.request_api.side_effect = [
        {"key": "12"},
        {
            "data": {
                "BANKACCTRECON": {
                    "RECORDNO": "12",
                    "MODE": "Manual",
                    "FEEDTYPE": None,
                    "STATE": "initiated",
                }
            }
        },
    ]

    with pytest.raises(FatalAPIError, match="MODE is 'Manual'"):
        create_bank_reconciliation(
            client,
            financial_entity="300_SVB",
            feed_date="09/22/2026",
            logger=logging.getLogger("test"),
        )


def test_resolve_missing_account_raises() -> None:
    client = MagicMock()
    client.location_id = None
    client.request_api.return_value = {
        "data": {
            "@count": "0",
            "checkingaccount": None,
        }
    }

    with pytest.raises(FatalAPIError, match="not found"):
        resolve_session_location_for_account(
            client,
            financial_entity="missing",
            logger=logging.getLogger("test"),
        )
