"""Tests for BankTransaction schema mapping."""

from __future__ import annotations

import pytest

from target_intacct.mappers.bank_transaction_schema_mapper import (
    BankTransactionSchemaMapper,
    InvalidBankTransactionError,
)
from target_intacct.xml_client import extract_xml_record_id, missing_xml_config_keys

SAMPLE_RECORD = {
    "bankAccountId": "BOV",
    "feedDate": "2020-05-15",
    "postingDate": "2020-04-09",
    "transactionType": "withdrawal",
    "documentType": "Check",
    "documentNumber": "198",
    "amount": -184.00,
    "description": "Office supplies",
    "payee": "Office Depot",
    "currency": "USD",
}


def test_map_bank_transaction_from_sample_payload() -> None:
    payload = BankTransactionSchemaMapper(SAMPLE_RECORD).to_intacct()

    assert payload == {
        "create": {
            "BANKACCTTXNFEED": {
                "FINANCIALENTITY": "BOV",
                "FEEDDATE": "05/15/2020",
                "FEEDTYPE": "xml",
                "FILENAME": "hotglue-BOV-05-15-2020",
                "BANKACCTTXNRECORDS": {
                    "BANKACCTTXNRECORD": {
                        "POSTINGDATE": "04/09/2020",
                        "TRANSACTIONTYPE": "withdrawal",
                        "DOCTYPE": "Check",
                        "DOCNO": "198",
                        "AMOUNT": "-184.00",
                        "DESCRIPTION": "Office supplies",
                        "PAYEE": "Office Depot",
                        "CURRENCY": "USD",
                    }
                },
            }
        },
        "_recon": {"feedDate": "05/15/2020"},
    }


def test_feed_date_defaults_when_omitted() -> None:
    record = {k: v for k, v in SAMPLE_RECORD.items() if k != "feedDate"}
    feed_date = BankTransactionSchemaMapper(record).to_intacct()["create"][
        "BANKACCTTXNFEED"
    ]["FEEDDATE"]
    assert len(feed_date) == 10
    assert feed_date[2] == "/" and feed_date[5] == "/"


def test_deposit_transaction_type() -> None:
    record = {**SAMPLE_RECORD, "transactionType": "deposit", "amount": 2000.0}
    txn = BankTransactionSchemaMapper(record).to_intacct()["create"]["BANKACCTTXNFEED"][
        "BANKACCTTXNRECORDS"
    ]["BANKACCTTXNRECORD"]
    assert txn["TRANSACTIONTYPE"] == "deposit"
    assert txn["AMOUNT"] == "2000.00"


def test_missing_bank_account_raises() -> None:
    record = {k: v for k, v in SAMPLE_RECORD.items() if k != "bankAccountId"}
    with pytest.raises(InvalidBankTransactionError, match="bankAccountId"):
        BankTransactionSchemaMapper(record).to_intacct()


def test_missing_amount_raises() -> None:
    record = {k: v for k, v in SAMPLE_RECORD.items() if k != "amount"}
    with pytest.raises(InvalidBankTransactionError, match="amount"):
        BankTransactionSchemaMapper(record).to_intacct()


def test_invalid_transaction_type_raises() -> None:
    record = {**SAMPLE_RECORD, "transactionType": "charge"}
    with pytest.raises(InvalidBankTransactionError, match="transactionType"):
        BankTransactionSchemaMapper(record).to_intacct()


def test_missing_xml_config_keys() -> None:
    assert missing_xml_config_keys({}) == [
        "company_id",
        "sender_id",
        "sender_password",
        "user_id",
        "user_password",
    ]
    assert (
        missing_xml_config_keys(
            {
                "company_id": "c",
                "sender_id": "s",
                "sender_password": "sp",
                "user_id": "u",
                "user_password": "up",
            }
        )
        == []
    )


def test_extract_xml_record_id() -> None:
    result = {
        "status": "success",
        "data": {"bankaccttxnfeed": {"RECORDNO": "91"}},
    }
    assert extract_xml_record_id(result, "BANKACCTTXNFEED") == "91"
    assert extract_xml_record_id({"key": "42"}, "BANKACCTTXNFEED") == "42"
