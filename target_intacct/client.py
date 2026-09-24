"""HTTP client and sink base classes for Intacct."""

from __future__ import annotations

import json
from typing import Any

from hotglue_singer_sdk.exceptions import FatalAPIError
from hotglue_singer_sdk.target_sdk.client import HotglueBaseSink, HotglueSink

from target_intacct.auth import IntacctAuthenticator

INTACCT_OAUTH_TOKEN_URL = "https://api.intacct.com/ia/api/v1/oauth2/token"


def parse_intacct_error_message(payload: Any) -> str | None:
    """Extract human-readable messages from an Intacct REST error body.

    Prefers unique ``details[].message`` values under ``ia::result.ia::error``.
    """
    if not isinstance(payload, dict):
        return None

    error = (payload.get("ia::result") or {}).get("ia::error") or payload.get("ia::error")
    if not isinstance(error, dict):
        return None

    details = error.get("details") or []
    detail_messages: list[str] = []
    seen: set[str] = set()
    for detail in details:
        if not isinstance(detail, dict):
            continue
        message = detail.get("message")
        if message and message not in seen:
            seen.add(message)
            detail_messages.append(str(message))

    if detail_messages:
        return "; ".join(detail_messages)

    top_level = error.get("message")
    return str(top_level) if top_level else None


class IntacctSink(HotglueBaseSink):
    """Intacct Base target sink class for sinks."""

    base_url = "https://api.intacct.com/ia/api/v1"
    auth_state = {}

    @property
    def authenticator(self) -> Any:
        # Local Intacct OAuth refresh only — no target.access_token_support
        # (that enables Hotglue /accesstoken refresh).
        return IntacctAuthenticator(
            self._target, self.auth_state, INTACCT_OAUTH_TOKEN_URL
        )

    @property
    def http_headers(self) -> dict:
        headers: dict[str, str] = {}
        entity_id = self.config.get("entity_id")
        if entity_id not in (None, ""):
            headers["X-IA-API-Param-Entity"] = str(entity_id)
        return headers

    def validate_response(self, response: Any) -> None:
        status_code = getattr(response, "status_code", 0)
        if 400 <= status_code < 500 and status_code != 429:
            raise FatalAPIError(self._client_error_message(response))
        super().validate_response(response)

    def _client_error_message(self, response: Any) -> str:
        try:
            payload = response.json()
        except (json.JSONDecodeError, ValueError, AttributeError):
            return getattr(response, "text", None) or self.response_error_message(response)

        parsed = parse_intacct_error_message(payload)
        return parsed or getattr(response, "text", None) or self.response_error_message(response)


class IntacctRecordSink(IntacctSink, HotglueSink):
    """Intacct Record target sink class for record sinks."""

    name = "IntacctRecordSink"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        # TODO: preprocess each record if needed (SDK calls this for every record).
        return record

    def upsert_record(self, record: dict, context: dict) -> None:
        # Called for each record on all sinks unless overridden.
        state_updates = {}
        pk_field = self.key_properties[0] if self.key_properties else "id"
        pk = record.get(pk_field)
        method = "POST"
        endpoint = self.endpoint

        if pk:
            method = "PATCH"
            endpoint = f"{endpoint}{pk}"
            state_updates["updated"] = True

        response = self.request_api(method, endpoint, request_data=record)
        record_id = pk or response.json().get("id")
        return record_id, True, state_updates
