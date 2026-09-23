"""Tests for CreditCardAccount schema mapping."""

from __future__ import annotations

import pytest

from target_intacct.mappers.credit_card_account_schema_mapper import (
    CreditCardAccountSchemaMapper,
    InvalidCreditCardAccountError,
)


SAMPLE_RECORD = {
    "id": None,
    "name": "CORP-VISA-001",
    "description": "Corporate travel Visa",
    "cardType": "visa",
    "accountType": "credit",
    "expirationMonth": "12",
    "expirationYear": "2028",
    "active": True,
    "vendorId": "VISA-VENDOR",
    "offsetGlAccountId": "2000",
    "locationId": "1",
    "billingAddress": {
        "line1": "300 Park Ave",
        "line2": "Suite 1400",
        "city": "San Jose",
        "state": "CA",
        "postalCode": "95110",
        "country": "United States",
        "countryCode": "US",
    },
}


def test_map_credit_card_account_from_sample_payload() -> None:
    mapper = CreditCardAccountSchemaMapper(SAMPLE_RECORD)

    assert mapper.endpoint() == "/objects/cash-management/credit-card-account"
    assert mapper.to_intacct() == {
        "id": "CORP-VISA-001",
        "accountDetails": {
            "cardType": "visa",
            "accountType": "credit",
            "expirationMonth": "12",
            "expirationYear": "2028",
            "description": "Corporate travel Visa",
            "billingAddress": {
                "addressLine1": "300 Park Ave",
                "addressLine2": "Suite 1400",
                "city": "San Jose",
                "state": "CA",
                "postCode": "95110",
                "country": "United States",
                "countryCode": "US",
            },
        },
        "accounting": {"offsetGLAccount": {"id": "2000"}},
        "vendor": {"id": "VISA-VENDOR"},
        "location": {"id": "1"},
        "status": "active",
    }


def test_normalize_amex_requires_card_number() -> None:
    with pytest.raises(InvalidCreditCardAccountError, match="cardNumber"):
        CreditCardAccountSchemaMapper(
            {
                **SAMPLE_RECORD,
                "cardType": "amex",
                "cardNumber": None,
            }
        ).to_intacct()


def test_expiration_date_shortcut() -> None:
    record = {
        **SAMPLE_RECORD,
        "expirationMonth": None,
        "expirationYear": None,
        "expirationDate": "2029-03",
    }
    payload = CreditCardAccountSchemaMapper(record).to_intacct()
    assert payload["accountDetails"]["expirationMonth"] == "03"
    assert payload["accountDetails"]["expirationYear"] == "2029"


def test_missing_vendor_raises() -> None:
    record = {k: v for k, v in SAMPLE_RECORD.items() if k != "vendorId"}
    with pytest.raises(InvalidCreditCardAccountError, match="vendor"):
        CreditCardAccountSchemaMapper(record).to_intacct()
