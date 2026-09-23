"""Map CreditCardTransaction records to Intacct cash-management payloads."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

ENDPOINT = "/objects/cash-management/credit-card-txn"

DIMENSION_FIELDS = (
    ("departmentId", "departmentKey", "department"),
    ("locationId", "locationKey", "location"),
    ("projectId", "projectKey", "project"),
    ("customerId", "customerKey", "customer"),
    ("vendorId", "vendorKey", "vendor"),
    ("employeeId", "employeeKey", "employee"),
    ("itemId", "itemKey", "item"),
    ("classId", "classKey", "class"),
)


class InvalidCreditCardTransactionError(ValueError):
    """Raised when a CreditCardTransaction record cannot be mapped to Intacct."""


class CreditCardTransactionSchemaMapper:
    """Map a CreditCardTransaction record to an Intacct create payload."""

    def __init__(self, record: dict[str, Any]) -> None:
        self.record = record

    def endpoint(self) -> str:
        """Return the Intacct REST endpoint for credit card transactions."""
        return ENDPOINT

    def to_intacct(self) -> dict[str, Any]:
        """Build the Intacct request body for creating a credit card transaction."""
        payload: dict[str, Any] = {
            "txnDate": self._map_txn_date(),
            "creditCardAccount": self._map_credit_card_account(),
            "lines": self._map_lines(),
        }

        reference_number = self.record.get("referenceNumber") or self.record.get(
            "transactionNumber"
        )
        if reference_number is not None:
            payload["referenceNumber"] = str(reference_number)

        payee = self.record.get("payee") or self.record.get("payeeName")
        if payee is not None:
            payload["payee"] = str(payee)

        description = self.record.get("description") or self.record.get("memo")
        if description is not None:
            payload["description"] = str(description)

        currency = self._map_currency()
        if currency:
            payload["currency"] = currency

        if self.record.get("isInclusiveTax") is not None:
            payload["isInclusiveTax"] = bool(self.record["isInclusiveTax"])

        return payload

    def _map_txn_date(self) -> str:
        raw = (
            self.record.get("transactionDate")
            or self.record.get("txnDate")
            or self.record.get("date")
        )
        if not raw:
            raise InvalidCreditCardTransactionError(
                "CreditCardTransaction requires `transactionDate`"
            )
        return self._format_date(raw, field_name="transactionDate")

    def _map_credit_card_account(self) -> dict[str, str]:
        account = self.record.get("creditCardAccount")
        if isinstance(account, dict):
            ref = self._object_ref(account)
            if ref:
                return ref

        for field in (
            "creditCardAccountId",
            "creditCardAccountKey",
            "accountId",
            "cardAccountId",
        ):
            value = self.record.get(field)
            if value not in (None, ""):
                key = "key" if field.endswith("Key") else "id"
                return {key: str(value)}

        raise InvalidCreditCardTransactionError(
            "CreditCardTransaction requires a credit card account via "
            "`creditCardAccountId`, `creditCardAccountKey`, `accountId`, or "
            "`creditCardAccount`"
        )

    def _map_currency(self) -> dict[str, Any] | None:
        currency = self.record.get("currency")
        if isinstance(currency, dict):
            mapped: dict[str, Any] = {}
            txn_currency = currency.get("txnCurrency") or currency.get("currency")
            if txn_currency:
                mapped["txnCurrency"] = str(txn_currency)
            base_currency = currency.get("baseCurrency")
            if base_currency:
                mapped["baseCurrency"] = str(base_currency)
            exchange_rate = currency.get("exchangeRate")
            if isinstance(exchange_rate, dict):
                mapped["exchangeRate"] = exchange_rate
            return mapped or None

        if currency not in (None, ""):
            return {"txnCurrency": str(currency)}

        txn_currency = self.record.get("txnCurrency")
        if txn_currency not in (None, ""):
            return {"txnCurrency": str(txn_currency)}

        return None

    def _map_lines(self) -> list[dict[str, Any]]:
        lines = self.record.get("lineItems") or self.record.get("lines") or []
        if not isinstance(lines, list) or not lines:
            raise InvalidCreditCardTransactionError(
                "CreditCardTransaction requires at least one entry in `lineItems`"
            )

        mapped_lines = []
        for index, line in enumerate(lines):
            if not isinstance(line, dict):
                raise InvalidCreditCardTransactionError(
                    f"CreditCardTransaction lineItems[{index}] must be an object"
                )
            mapped_lines.append(self._map_line(line, index))
        return mapped_lines

    def _map_line(self, line: dict[str, Any], index: int) -> dict[str, Any]:
        amount = line.get("amount")
        if amount is None:
            amount = line.get("txnAmount")
        if amount is None:
            amount = line.get("totalTxnAmount")
        if amount is None:
            raise InvalidCreditCardTransactionError(
                f"CreditCardTransaction lineItems[{index}] requires `amount`"
            )

        amount_str = self._format_amount(amount)
        mapped: dict[str, Any] = {
            "txnAmount": amount_str,
            "totalTxnAmount": amount_str,
            "glAccount": self._map_line_gl_account(line, index),
        }

        description = line.get("description") or line.get("memo")
        if description is not None:
            mapped["description"] = str(description)

        if line.get("isBillable") is not None:
            mapped["isBillable"] = bool(line["isBillable"])
        if line.get("isBilled") is not None:
            mapped["isBilled"] = bool(line["isBilled"])

        dimensions = self._map_dimensions(line)
        if dimensions:
            mapped["dimensions"] = dimensions

        return mapped

    def _map_line_gl_account(self, line: dict[str, Any], index: int) -> dict[str, str]:
        gl_account = line.get("glAccount")
        if isinstance(gl_account, dict):
            ref = self._object_ref(gl_account)
            if ref:
                return ref

        for field in ("glAccountId", "glAccountKey", "accountId"):
            value = line.get(field)
            if value not in (None, ""):
                key = "key" if field.endswith("Key") else "id"
                return {key: str(value)}

        raise InvalidCreditCardTransactionError(
            f"CreditCardTransaction lineItems[{index}] requires `glAccountId`, "
            "`glAccountKey`, `accountId`, or `glAccount`"
        )

    def _map_dimensions(self, line: dict[str, Any]) -> dict[str, dict[str, str]] | None:
        dimensions: dict[str, dict[str, str]] = {}

        nested = line.get("dimensions")
        if isinstance(nested, dict):
            for dim_name, dim_value in nested.items():
                if isinstance(dim_value, dict):
                    ref = self._object_ref(dim_value)
                    if ref:
                        dimensions[dim_name] = ref
                elif dim_value not in (None, ""):
                    dimensions[dim_name] = {"id": str(dim_value)}

        for id_field, key_field, dim_name in DIMENSION_FIELDS:
            if dim_name in dimensions:
                continue
            if line.get(key_field) not in (None, ""):
                dimensions[dim_name] = {"key": str(line[key_field])}
            elif line.get(id_field) not in (None, ""):
                dimensions[dim_name] = {"id": str(line[id_field])}

        return dimensions or None

    @staticmethod
    def _format_date(value: Any, field_name: str) -> str:
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()

        text = str(value).strip()
        if "T" in text:
            text = text.split("T", 1)[0]
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            return text[:10]

        raise InvalidCreditCardTransactionError(
            f"CreditCardTransaction `{field_name}` must be an ISO date (YYYY-MM-DD), "
            f"got '{value}'"
        )

    @staticmethod
    def _format_amount(value: Any) -> str:
        try:
            return f"{float(value):.2f}"
        except (TypeError, ValueError) as exc:
            raise InvalidCreditCardTransactionError(
                f"Invalid amount '{value}'"
            ) from exc

    @staticmethod
    def _object_ref(value: dict[str, Any]) -> dict[str, str] | None:
        if value.get("key") not in (None, ""):
            return {"key": str(value["key"])}
        if value.get("id") not in (None, ""):
            return {"id": str(value["id"])}
        return None
