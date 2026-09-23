"""Tests for BankAccount schema mapping."""

from __future__ import annotations

import pytest

from target_intacct.mappers.bank_account_schema_mapper import (
    BankAccountSchemaMapper,
    InvalidBankAccountError,
)


def test_map_checking_account_from_unified_payload() -> None:
    record = {
        "id": None,
        "name": "test",
        "accountNumber": "6368098596",
        "routingNumber": "051502395",
        "type": "checking",
        "currency": "USD",
        "holderName": "Not A Real Company Inc.",
        "bankName": "Test Bank",
        "locationId": "1",
        "glAccountId": "1000",
        "active": True,
    }

    mapper = BankAccountSchemaMapper(record)

    assert mapper.endpoint() == "/objects/cash-management/checking-account"
    assert mapper.to_intacct() == {
        "id": "test",
        "bankAccountDetails": {
            "bankName": "Test Bank",
            "currency": "USD",
            "accountNumber": "6368098596",
            "routingNumber": "051502395",
            "accountHolderName": "Not A Real Company Inc.",
        },
        "accounting": {"glAccount": {"id": "1000"}},
        "location": {"id": "1"},
        "status": "active",
    }


def test_map_savings_account_omits_holder_name() -> None:
    record = {
        "name": "SAVE1",
        "type": "savings",
        "currency": "USD",
        "bankName": "Savings Bank",
        "holderName": "Should Be Ignored",
        "locationId": "1",
        "glAccountId": "2000",
    }

    mapper = BankAccountSchemaMapper(record)

    assert mapper.endpoint() == "/objects/cash-management/savings-account"
    payload = mapper.to_intacct()
    assert "accountHolderName" not in payload["bankAccountDetails"]


def test_missing_location_raises() -> None:
    with pytest.raises(InvalidBankAccountError, match="location"):
        BankAccountSchemaMapper(
            {
                "name": "test",
                "currency": "USD",
                "bankName": "Test Bank",
                "glAccountId": "1000",
            }
        ).to_intacct()


def test_unsupported_type_raises() -> None:
    with pytest.raises(InvalidBankAccountError, match="Unsupported BankAccount type"):
        BankAccountSchemaMapper({"name": "test", "type": "money-market"})
