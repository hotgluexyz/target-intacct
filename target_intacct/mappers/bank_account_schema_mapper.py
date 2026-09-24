"""Map unified BankAccount records to Intacct cash-management payloads."""

from __future__ import annotations

from typing import Any

ACCOUNT_TYPE_ENDPOINTS = {
    "checking": "/objects/cash-management/checking-account",
    "savings": "/objects/cash-management/savings-account",
}


class InvalidBankAccountError(ValueError):
    """Raised when a BankAccount record cannot be mapped to Intacct."""


class BankAccountSchemaMapper:
    """Map a unified BankAccount record to an Intacct create payload."""

    def __init__(self, record: dict[str, Any]) -> None:
        self.record = record
        self.account_type = self._resolve_account_type()

    def endpoint(self) -> str:
        """Return the Intacct REST endpoint for this account type."""
        return ACCOUNT_TYPE_ENDPOINTS[self.account_type]

    def to_intacct(self) -> dict[str, Any]:
        """Build the Intacct request body for creating a bank account."""
        account_id = self.record.get("id") or self.record.get("name")
        if not account_id:
            raise InvalidBankAccountError(
                "BankAccount requires `id` or `name` to use as the Intacct account id"
            )

        bank_name = self.record.get("bankName") or self.record.get("name")
        if not bank_name:
            raise InvalidBankAccountError(
                "BankAccount requires `bankName` (or `name`) for bankAccountDetails.bankName"
            )

        currency = self.record.get("currency")
        if not currency:
            raise InvalidBankAccountError("BankAccount requires `currency`")

        payload: dict[str, Any] = {
            "id": str(account_id),
            "bankAccountDetails": self._map_bank_account_details(bank_name, currency),
            "accounting": {"glAccount": self._map_gl_account()},
            "location": self._map_location(),
        }

        status = self._map_status()
        if status:
            payload["status"] = status

        # Optional override only — default rule set is assigned via XML after create
        # (top-level 001 / entity HG-MATCH). Sending REST reconciliation.ruleSet id
        # "001" fails for entity-owned checking accounts.
        rule_set = self._map_rule_set()
        if rule_set:
            payload["reconciliation"] = {"ruleSet": rule_set}

        return payload

    def _map_rule_set(self) -> dict[str, str] | None:
        for field in ("ruleSetId", "ruleSetKey", "reconciliationRuleSetId"):
            value = self.record.get(field)
            if value not in (None, ""):
                key = "key" if field == "ruleSetKey" else "id"
                return {key: str(value)}

        rule_set = self.record.get("ruleSet")
        if isinstance(rule_set, dict):
            return self._object_ref(rule_set)

        return None

    def _resolve_account_type(self) -> str:
        raw_type = (self.record.get("type") or "checking").strip().lower()
        if raw_type not in ACCOUNT_TYPE_ENDPOINTS:
            supported = ", ".join(sorted(ACCOUNT_TYPE_ENDPOINTS))
            raise InvalidBankAccountError(
                f"Unsupported BankAccount type '{self.record.get('type')}'. "
                f"Supported types: {supported}"
            )
        return raw_type

    def _map_bank_account_details(self, bank_name: str, currency: str) -> dict[str, Any]:
        details: dict[str, Any] = {
            "bankName": bank_name,
            "currency": currency,
        }

        account_number = self.record.get("accountNumber")
        if account_number is not None:
            details["accountNumber"] = str(account_number)

        routing_number = self.record.get("routingNumber")
        if routing_number is not None:
            details["routingNumber"] = str(routing_number)

        # accountHolderName is documented for checking accounts only.
        if self.account_type == "checking":
            holder_name = self.record.get("holderName")
            if holder_name is not None:
                details["accountHolderName"] = holder_name

        return details

    def _map_gl_account(self) -> dict[str, str]:
        gl_account = self.record.get("glAccount")
        if isinstance(gl_account, dict):
            ref = self._object_ref(gl_account)
            if ref:
                return ref

        for field in ("glAccountId", "glAccountKey", "accountId"):
            value = self.record.get(field)
            if value not in (None, ""):
                key = "key" if field == "glAccountKey" else "id"
                return {key: str(value)}

        raise InvalidBankAccountError(
            "BankAccount requires a GL account via `glAccountId`, `glAccountKey`, "
            "`accountId`, or `glAccount`"
        )

    def _map_location(self) -> dict[str, str]:
        location = self.record.get("location")
        if isinstance(location, dict):
            ref = self._object_ref(location)
            if ref:
                return ref

        for field in ("locationId", "locationKey", "subsidiaryId", "subsidiaryNumber"):
            value = self.record.get(field)
            if value not in (None, ""):
                key = "key" if field == "locationKey" else "id"
                return {key: str(value)}

        raise InvalidBankAccountError(
            "BankAccount requires a location via `locationId`, `locationKey`, "
            "`subsidiaryId`, `subsidiaryNumber`, or `location`"
        )

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
