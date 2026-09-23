"""Tests for CreditCardTransaction schema mapping."""

from __future__ import annotations

import pytest

from target_intacct.mappers.credit_card_transaction_schema_mapper import (
    CreditCardTransactionSchemaMapper,
    InvalidCreditCardTransactionError,
)


SAMPLE_RECORD = {
    "transactionDate": "2025-04-11",
    "creditCardAccountId": "CORP-VISA-001",
    "referenceNumber": "CCTxn-001",
    "payee": "Airline Co",
    "description": "Travel expenses",
    "currency": "USD",
    "lineItems": [
        {
            "amount": 125.5,
            "glAccountId": "6000",
            "description": "Flight to SFO",
            "departmentId": "PS",
            "locationId": "1",
            "isBillable": False,
        }
    ],
}


def test_map_credit_card_transaction_from_sample_payload() -> None:
    mapper = CreditCardTransactionSchemaMapper(SAMPLE_RECORD)

    assert mapper.endpoint() == "/objects/cash-management/credit-card-txn"
    assert mapper.to_intacct() == {
        "txnDate": "2025-04-11",
        "creditCardAccount": {"id": "CORP-VISA-001"},
        "referenceNumber": "CCTxn-001",
        "payee": "Airline Co",
        "description": "Travel expenses",
        "currency": {"txnCurrency": "USD"},
        "lines": [
            {
                "txnAmount": "125.50",
                "totalTxnAmount": "125.50",
                "glAccount": {"id": "6000"},
                "description": "Flight to SFO",
                "isBillable": False,
                "dimensions": {
                    "department": {"id": "PS"},
                    "location": {"id": "1"},
                },
            }
        ],
    }


def test_missing_line_items_raises() -> None:
    record = {k: v for k, v in SAMPLE_RECORD.items() if k != "lineItems"}
    with pytest.raises(InvalidCreditCardTransactionError, match="lineItems"):
        CreditCardTransactionSchemaMapper(record).to_intacct()


def test_datetime_transaction_date_is_normalized() -> None:
    record = {**SAMPLE_RECORD, "transactionDate": "2025-04-11T15:30:00Z"}
    payload = CreditCardTransactionSchemaMapper(record).to_intacct()
    assert payload["txnDate"] == "2025-04-11"


def test_missing_credit_card_account_raises() -> None:
    record = {k: v for k, v in SAMPLE_RECORD.items() if k != "creditCardAccountId"}
    with pytest.raises(InvalidCreditCardTransactionError, match="credit card account"):
        CreditCardTransactionSchemaMapper(record).to_intacct()
