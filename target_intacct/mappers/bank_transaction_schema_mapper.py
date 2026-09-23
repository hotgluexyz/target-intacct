"""Map BankTransaction records to Intacct XML bank feed create payloads."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

TRANSACTION_TYPES = frozenset({"withdrawal", "deposit"})


class InvalidBankTransactionError(ValueError):
    """Raised when a BankTransaction record cannot be mapped to Intacct."""


class BankTransactionSchemaMapper:
    """Map a BankTransaction record to an Intacct XML create bank-feed payload.

    Each record becomes one ``BANKACCTTXNFEED`` with a single ``BANKACCTTXNRECORD``.
    See: https://developer.intacct.com/api/cash-management/bank-feeds/#create-bank-feed
    """

    def __init__(self, record: dict[str, Any]) -> None:
        self.record = record

    def to_intacct(self) -> dict[str, Any]:
        """Build the XML function body for creating a bank feed."""
        financial_entity = self._map_financial_entity()
        feed_date = self._map_feed_date()
        feed: dict[str, Any] = {
            "FINANCIALENTITY": financial_entity,
            "FEEDDATE": feed_date,
            "FEEDTYPE": "xml",
            "BANKACCTTXNRECORDS": {
                "BANKACCTTXNRECORD": self._map_txn_record(),
            },
        }

        filename = self.record.get("filename") or self.record.get("fileName")
        if filename not in (None, ""):
            feed["FILENAME"] = str(filename)
        else:
            # Shown in the UI for AutomatchReview reconciliations.
            feed["FILENAME"] = f"hotglue-{financial_entity}-{feed_date.replace('/', '-')}"

        payload: dict[str, Any] = {
            "create": {"BANKACCTTXNFEED": feed},
            "_recon": self._map_recon_options(feed_date),
        }
        return payload

    def _map_recon_options(self, feed_date: str) -> dict[str, Any]:
        """Optional reconciliation overrides carried through to the sink."""
        options: dict[str, Any] = {"feedDate": feed_date}
        for source, dest in (
            ("stmtEndingDate", "stmtEndingDate"),
            ("statementEndingDate", "stmtEndingDate"),
            ("cutoffDate", "cutoffDate"),
            ("stmtEndingBalance", "stmtEndingBalance"),
            ("statementEndingBalance", "stmtEndingBalance"),
            ("reconMode", "mode"),
            ("reconciliationMode", "mode"),
        ):
            value = self.record.get(source)
            if value not in (None, ""):
                options[dest] = value
        return options

    def _map_financial_entity(self) -> str:
        for field in (
            "bankAccountId",
            "accountId",
            "financialEntity",
            "financialEntityId",
        ):
            value = self.record.get(field)
            if value not in (None, ""):
                return str(value)

        account = self.record.get("bankAccount")
        if isinstance(account, dict):
            for field in ("id", "key"):
                if account.get(field) not in (None, ""):
                    return str(account[field])

        raise InvalidBankTransactionError(
            "BankTransaction requires `bankAccountId`, `accountId`, or `bankAccount`"
        )

    def _map_feed_date(self) -> str:
        raw = self.record.get("feedDate")
        if raw in (None, ""):
            return self._format_xml_date(
                datetime.now(tz=timezone.utc).date(),
                field_name="feedDate",
            )
        return self._format_xml_date(raw, field_name="feedDate")

    def _map_txn_record(self) -> dict[str, Any]:
        txn: dict[str, Any] = {
            "TRANSACTIONTYPE": self._map_transaction_type(),
            "AMOUNT": self._map_amount(),
        }

        posting_date = (
            self.record.get("postingDate")
            or self.record.get("transactionDate")
            or self.record.get("date")
        )
        if posting_date not in (None, ""):
            txn["POSTINGDATE"] = self._format_xml_date(
                posting_date, field_name="postingDate"
            )

        doc_type = self.record.get("documentType") or self.record.get("docType")
        if doc_type not in (None, ""):
            txn["DOCTYPE"] = str(doc_type)

        doc_no = (
            self.record.get("documentNumber")
            or self.record.get("docNo")
            or self.record.get("referenceNumber")
            or self.record.get("transactionNumber")
        )
        if doc_no not in (None, ""):
            txn["DOCNO"] = str(doc_no)

        payee = self.record.get("payee") or self.record.get("payeeName")
        if payee not in (None, ""):
            txn["PAYEE"] = str(payee)

        description = self.record.get("description") or self.record.get("memo")
        if description not in (None, ""):
            txn["DESCRIPTION"] = str(description)

        currency = self.record.get("currency")
        if isinstance(currency, dict):
            currency = currency.get("txnCurrency") or currency.get("currency")
        if currency not in (None, ""):
            txn["CURRENCY"] = str(currency)

        return txn

    def _map_transaction_type(self) -> str:
        raw = self.record.get("transactionType") or self.record.get("type")
        if raw in (None, ""):
            raise InvalidBankTransactionError(
                "BankTransaction requires `transactionType` "
                f"({' or '.join(sorted(TRANSACTION_TYPES))})"
            )

        normalized = str(raw).strip().lower()
        if normalized not in TRANSACTION_TYPES:
            raise InvalidBankTransactionError(
                f"Unsupported transactionType '{raw}'. "
                f"Supported: {', '.join(sorted(TRANSACTION_TYPES))}"
            )
        return normalized

    def _map_amount(self) -> str:
        amount = self.record.get("amount")
        if amount is None:
            raise InvalidBankTransactionError("BankTransaction requires `amount`")
        try:
            return f"{float(amount):.2f}"
        except (TypeError, ValueError) as exc:
            raise InvalidBankTransactionError(f"Invalid amount '{amount}'") from exc

    @staticmethod
    def _format_xml_date(value: Any, field_name: str) -> str:
        """Format a date as mm/dd/yyyy for the Intacct XML API."""
        if isinstance(value, datetime):
            return value.date().strftime("%m/%d/%Y")
        if isinstance(value, date):
            return value.strftime("%m/%d/%Y")

        text = str(value).strip()
        if "T" in text:
            text = text.split("T", 1)[0]
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            parsed = date.fromisoformat(text[:10])
            return parsed.strftime("%m/%d/%Y")
        if len(text) >= 10 and text[2] == "/" and text[5] == "/":
            return text[:10]

        raise InvalidBankTransactionError(
            f"BankTransaction `{field_name}` must be an ISO date (YYYY-MM-DD) "
            f"or mm/dd/yyyy, got '{value}'"
        )
