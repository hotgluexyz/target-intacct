"""Intacct target sink class, which handles writing streams."""

from __future__ import annotations

from typing import Any

from hotglue_models_accounting.accounting import BankAccount

from hotglue_singer_sdk.exceptions import FatalAPIError

from target_intacct.bank_reconciliation import ensure_open_bank_reconciliation
from target_intacct.client import IntacctRecordSink
from target_intacct.mappers.bank_account_schema_mapper import BankAccountSchemaMapper
from target_intacct.mappers.bank_transaction_schema_mapper import (
    BankTransactionSchemaMapper,
)
from target_intacct.mappers.credit_card_account_schema_mapper import (
    CreditCardAccountSchemaMapper,
)
from target_intacct.mappers.credit_card_transaction_schema_mapper import (
    CreditCardTransactionSchemaMapper,
)
from target_intacct.xml_client import IntacctXmlClient, extract_xml_record_id


class BankAccountSink(IntacctRecordSink):
    """BankAccount sink: unified payload → Intacct checking/savings account."""

    name = "BankAccount"
    endpoint = "/objects/cash-management/checking-account"
    unified_schema = BankAccount
    # Keep False so Intacct-specific fields (routingNumber, locationId, glAccountId)
    # are not stripped by unified-schema validation.
    auto_validate_unified_schema = False

    def preprocess_record(self, record: dict, context: dict) -> dict:
        mapper = BankAccountSchemaMapper(record)
        payload = mapper.to_intacct()
        payload["_endpoint"] = mapper.endpoint()
        return payload

    def upsert_record(self, record: dict, context: dict) -> tuple[Any, bool, dict]:
        state_updates: dict = {}
        endpoint = record.pop("_endpoint", self.endpoint)
        response = self.request_api("POST", endpoint, request_data=record)

        result = response.json().get("ia::result") or {}
        record_id = result.get("key") or result.get("id")
        return record_id, True, state_updates


class CreditCardAccountSink(IntacctRecordSink):
    """CreditCardAccount sink: unified payload → Intacct credit card account."""

    name = "CreditCardAccount"
    endpoint = "/objects/cash-management/credit-card-account"
    auto_validate_unified_schema = False

    def preprocess_record(self, record: dict, context: dict) -> dict:
        mapper = CreditCardAccountSchemaMapper(record)
        payload = mapper.to_intacct()
        payload["_endpoint"] = mapper.endpoint()
        return payload

    def upsert_record(self, record: dict, context: dict) -> tuple[Any, bool, dict]:
        state_updates: dict = {}
        endpoint = record.pop("_endpoint", self.endpoint)
        response = self.request_api("POST", endpoint, request_data=record)

        result = response.json().get("ia::result") or {}
        record_id = result.get("key") or result.get("id")
        return record_id, True, state_updates


class BankTransactionSink(IntacctRecordSink):
    """BankTransaction sink: unified payload → Intacct XML bank feed (single txn)."""

    name = "BankTransaction"
    endpoint = ""
    auto_validate_unified_schema = False

    @property
    def xml_client(self) -> IntacctXmlClient:
        client = getattr(self, "_xml_client", None)
        if client is None:
            client = IntacctXmlClient(self.config, self.logger)
            self._xml_client = client
        return client

    @property
    def _open_recon_cache(self) -> dict[str, str]:
        cache = getattr(self, "_open_recon_cache_data", None)
        if cache is None:
            cache = {}
            self._open_recon_cache_data = cache
        return cache

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return BankTransactionSchemaMapper(record).to_intacct()

    def upsert_record(self, record: dict, context: dict) -> tuple[Any, bool, dict]:
        state_updates: dict = {}
        recon_options = record.pop("_recon", {}) or {}
        feed = (record.get("create") or {}).get("BANKACCTTXNFEED") or {}
        financial_entity = feed.get("FINANCIALENTITY")
        feed_date = feed.get("FEEDDATE") or recon_options.get("feedDate")

        if not financial_entity or not feed_date:
            raise FatalAPIError(
                "BankTransaction feed payload missing FINANCIALENTITY or FEEDDATE"
            )

        # XML bank-feed lines only show in the UI when an open AutomatchReview
        # reconciliation exists for the account.
        recon_id = ensure_open_bank_reconciliation(
            self.xml_client,
            financial_entity=str(financial_entity),
            feed_date=str(feed_date),
            logger=self.logger,
            mode=str(recon_options.get("mode") or "AutomatchReview"),
            stmt_ending_date=(
                str(recon_options["stmtEndingDate"])
                if recon_options.get("stmtEndingDate") not in (None, "")
                else None
            ),
            cutoff_date=(
                str(recon_options["cutoffDate"])
                if recon_options.get("cutoffDate") not in (None, "")
                else None
            ),
            stmt_ending_balance=(
                str(recon_options["stmtEndingBalance"])
                if recon_options.get("stmtEndingBalance") not in (None, "")
                else None
            ),
            cache=self._open_recon_cache,
        )
        state_updates["bankAcctReconId"] = recon_id

        result = self.xml_client.request_api(record)
        record_id = extract_xml_record_id(result, "BANKACCTTXNFEED")
        if not record_id:
            raise FatalAPIError(
                f"Bank feed create succeeded but no RECORDNO found: {result}"
            )
        return record_id, True, state_updates


class CreditCardTransactionSink(IntacctRecordSink):
    """CreditCardTransaction sink: unified payload → Intacct credit card txn."""

    name = "CreditCardTransaction"
    endpoint = "/objects/cash-management/credit-card-txn"
    auto_validate_unified_schema = False

    def preprocess_record(self, record: dict, context: dict) -> dict:
        mapper = CreditCardTransactionSchemaMapper(record)
        payload = mapper.to_intacct()
        payload["_endpoint"] = mapper.endpoint()
        return payload

    def upsert_record(self, record: dict, context: dict) -> tuple[Any, bool, dict]:
        state_updates: dict = {}
        endpoint = record.pop("_endpoint", self.endpoint)
        response = self.request_api("POST", endpoint, request_data=record)

        result = response.json().get("ia::result") or {}
        record_id = result.get("key") or result.get("id")
        return record_id, True, state_updates
