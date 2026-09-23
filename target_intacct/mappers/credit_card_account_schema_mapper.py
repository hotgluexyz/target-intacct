"""Map CreditCardAccount records to Intacct cash-management payloads."""

from __future__ import annotations

from typing import Any

ENDPOINT = "/objects/cash-management/credit-card-account"

CARD_TYPES = {
    "visa": "visa",
    "mastercard": "mastercard",
    "master card": "mastercard",
    "discover": "discover",
    "americanexpress": "americanExpress",
    "american express": "americanExpress",
    "amex": "americanExpress",
    "dinersclub": "dinersClub",
    "diners club": "dinersClub",
    "otherchargecard": "otherChargeCard",
    "other charge card": "otherChargeCard",
    "other": "otherChargeCard",
}

ACCOUNT_TYPES = {"credit", "debit"}


class InvalidCreditCardAccountError(ValueError):
    """Raised when a CreditCardAccount record cannot be mapped to Intacct."""


class CreditCardAccountSchemaMapper:
    """Map a CreditCardAccount record to an Intacct create payload."""

    def __init__(self, record: dict[str, Any]) -> None:
        self.record = record

    def endpoint(self) -> str:
        """Return the Intacct REST endpoint for credit card accounts."""
        return ENDPOINT

    def to_intacct(self) -> dict[str, Any]:
        """Build the Intacct request body for creating a credit card account."""
        account_id = self.record.get("id") or self.record.get("name")
        if not account_id:
            raise InvalidCreditCardAccountError(
                "CreditCardAccount requires `id` or `name` to use as the Intacct account id"
            )

        payload: dict[str, Any] = {
            "id": str(account_id),
            "accountDetails": self._map_account_details(),
            "accounting": {"offsetGLAccount": self._map_offset_gl_account()},
            "vendor": self._map_vendor(),
        }

        location = self._map_optional_ref(
            ("locationId", "locationKey", "subsidiaryId", "subsidiaryNumber"),
            "location",
        )
        if location:
            payload["location"] = location

        department = self._map_optional_ref(("departmentId", "departmentKey"), "department")
        if department:
            payload["department"] = department

        status = self._map_status()
        if status:
            payload["status"] = status

        return payload

    def _map_account_details(self) -> dict[str, Any]:
        card_type = self._resolve_card_type()
        account_type = self._resolve_account_type()
        expiration_month, expiration_year = self._resolve_expiration()

        details: dict[str, Any] = {
            "cardType": card_type,
            "accountType": account_type,
            "expirationMonth": expiration_month,
            "expirationYear": expiration_year,
        }

        description = self.record.get("description")
        if description is not None:
            details["description"] = description

        card_number = self.record.get("cardNumber") or self.record.get("number")
        if card_number is not None:
            details["number"] = str(card_number)
        elif card_type == "americanExpress":
            raise InvalidCreditCardAccountError(
                "CreditCardAccount requires `cardNumber` when cardType is americanExpress"
            )

        billing_address = self._map_billing_address()
        if billing_address:
            details["billingAddress"] = billing_address

        if account_type == "debit":
            checking = self._map_optional_ref(
                ("checkingAccountId", "checkingAccountKey", "bankAccountId"),
                "debitCardCheckingAccount",
            )
            if checking:
                details["debitCardCheckingAccount"] = checking

        return details

    def _resolve_card_type(self) -> str:
        raw = self.record.get("cardType")
        if not raw:
            raise InvalidCreditCardAccountError("CreditCardAccount requires `cardType`")
        normalized = CARD_TYPES.get(str(raw).strip().lower())
        if not normalized:
            supported = ", ".join(sorted(set(CARD_TYPES.values())))
            raise InvalidCreditCardAccountError(
                f"Unsupported cardType '{raw}'. Supported types: {supported}"
            )
        return normalized

    def _resolve_account_type(self) -> str:
        raw = (self.record.get("accountType") or "credit").strip().lower()
        if raw not in ACCOUNT_TYPES:
            raise InvalidCreditCardAccountError(
                f"Unsupported accountType '{self.record.get('accountType')}'. "
                f"Supported types: {', '.join(sorted(ACCOUNT_TYPES))}"
            )
        return raw

    def _resolve_expiration(self) -> tuple[str, str]:
        month = self.record.get("expirationMonth")
        year = self.record.get("expirationYear")

        expiration_date = self.record.get("expirationDate")
        if expiration_date and (month is None or year is None):
            # Accept YYYY-MM or MM/YYYY
            text = str(expiration_date).strip()
            if "-" in text:
                parts = text.split("-")
                if len(parts) >= 2:
                    year = year or parts[0]
                    month = month or parts[1]
            elif "/" in text:
                parts = text.split("/")
                if len(parts) >= 2:
                    month = month or parts[0]
                    year = year or parts[1]

        if month is None or year is None:
            raise InvalidCreditCardAccountError(
                "CreditCardAccount requires `expirationMonth` and `expirationYear` "
                "(or `expirationDate`)"
            )

        try:
            month_int = int(str(month).strip())
        except ValueError as exc:
            raise InvalidCreditCardAccountError(
                f"Invalid expirationMonth '{month}'"
            ) from exc

        if month_int < 1 or month_int > 12:
            raise InvalidCreditCardAccountError(
                f"expirationMonth must be between 1 and 12, got '{month}'"
            )

        return f"{month_int:02d}", str(year).strip()

    def _map_billing_address(self) -> dict[str, Any] | None:
        address = self.record.get("billingAddress") or self.record.get("address")
        if not isinstance(address, dict):
            return None

        mapped = {
            "addressLine1": address.get("addressLine1") or address.get("line1"),
            "addressLine2": address.get("addressLine2") or address.get("line2"),
            "addressLine3": address.get("addressLine3") or address.get("line3"),
            "city": address.get("city"),
            "state": address.get("state"),
            "postCode": address.get("postCode") or address.get("postalCode"),
            "country": address.get("country"),
            "countryCode": address.get("countryCode"),
        }
        cleaned = {key: value for key, value in mapped.items() if value not in (None, "")}
        return cleaned or None

    def _map_offset_gl_account(self) -> dict[str, str]:
        offset = self.record.get("offsetGlAccount") or self.record.get("glAccount")
        if isinstance(offset, dict):
            ref = self._object_ref(offset)
            if ref:
                return ref

        for field in (
            "offsetGlAccountId",
            "offsetGlAccountKey",
            "glAccountId",
            "glAccountKey",
            "accountId",
        ):
            value = self.record.get(field)
            if value not in (None, ""):
                key = "key" if field.endswith("Key") else "id"
                return {key: str(value)}

        raise InvalidCreditCardAccountError(
            "CreditCardAccount requires an offset GL account via `offsetGlAccountId`, "
            "`glAccountId`, `offsetGlAccountKey`, or `offsetGlAccount`"
        )

    def _map_vendor(self) -> dict[str, str]:
        vendor = self.record.get("vendor")
        if isinstance(vendor, dict):
            ref = self._object_ref(vendor)
            if ref:
                return ref

        for field in ("vendorId", "vendorKey", "vendorName", "vendorNumber"):
            value = self.record.get(field)
            if value not in (None, ""):
                key = "key" if field == "vendorKey" else "id"
                return {key: str(value)}

        raise InvalidCreditCardAccountError(
            "CreditCardAccount requires a vendor via `vendorId`, `vendorKey`, "
            "`vendorName`, `vendorNumber`, or `vendor`"
        )

    def _map_optional_ref(
        self,
        fields: tuple[str, ...],
        object_field: str,
    ) -> dict[str, str] | None:
        value = self.record.get(object_field)
        if isinstance(value, dict):
            ref = self._object_ref(value)
            if ref:
                return ref

        for field in fields:
            field_value = self.record.get(field)
            if field_value not in (None, ""):
                key = "key" if field.endswith("Key") else "id"
                return {key: str(field_value)}
        return None

    def _map_status(self) -> str | None:
        active = self.record.get("active")
        if active is None:
            return None
        return "active" if active else "inactive"

    @staticmethod
    def _object_ref(value: dict[str, Any]) -> dict[str, str] | None:
        if value.get("key") not in (None, ""):
            return {"key": str(value["key"])}
        if value.get("id") not in (None, ""):
            return {"id": str(value["id"])}
        return None
