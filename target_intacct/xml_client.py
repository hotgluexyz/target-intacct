"""Intacct XML Web Services client (session login + function requests)."""

from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Any

import backoff
import requests
import xmltodict
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError

XML_BASE_URL = "https://api.intacct.com/ia/xml/xmlgw.phtml"
XML_CONFIG_KEYS = (
    "company_id",
    "sender_id",
    "sender_password",
    "user_id",
    "user_password",
)


def missing_xml_config_keys(config: dict[str, Any]) -> list[str]:
    """Return required XML auth keys that are missing or empty."""
    return [key for key in XML_CONFIG_KEYS if config.get(key) in (None, "")]


class IntacctXmlClient:
    """Minimal XML gateway client for Intacct functions not on the REST API."""

    def __init__(self, config: dict[str, Any], logger: logging.Logger) -> None:
        missing = missing_xml_config_keys(config)
        if missing:
            raise FatalAPIError(
                "BankTransaction requires XML API credentials. Missing config: "
                + ", ".join(missing)
            )

        self.config = config
        self.logger = logger
        self.session_id: str | None = None
        self.session_timeout_timestamp: int | None = None
        self._location_id = self._resolve_location_id(config)
        self.login()

    @staticmethod
    def _resolve_location_id(config: dict[str, Any]) -> str | None:
        for field in ("entity_id", "location_id"):
            value = config.get(field)
            if value not in (None, ""):
                return str(value)
        return None

    @property
    def location_id(self) -> str | None:
        """Entity/location for XML login, or None for top-level."""
        return self._location_id

    @location_id.setter
    def location_id(self, value: str | None) -> None:
        if value in ("TOP_LEVEL", ""):
            value = None
        if value != self._location_id:
            self.session_id = None
            self.session_timeout_timestamp = None
            self._location_id = value

    @property
    def http_headers(self) -> dict[str, str]:
        return {"content-type": "application/xml"}

    def login(self) -> None:
        xml_body = xmltodict.unparse(self._login_request_body()).encode("utf-8")
        try:
            response = requests.post(
                XML_BASE_URL,
                headers=self.http_headers,
                data=xml_body,
                timeout=60,
            )
            self.validate_response(response)
            operation = self.parse_response(response)["response"]["operation"]
            if operation["authentication"]["status"] != "success":
                raise FatalAPIError(f"XML login failed: {operation}")

            session_details = operation["result"]["data"]["api"]
            self.session_id = session_details["sessionid"]
            self.session_timeout_timestamp = self._session_timeout(operation)
        except requests.RequestException as exc:
            raise FatalAPIError(f"XML login request failed: {exc!r}") from exc
        except KeyError as exc:
            raise FatalAPIError(f"Unexpected XML login response: {exc!r}") from exc

    def request_api(self, request_data: dict[str, Any]) -> dict[str, Any]:
        """Send a single XML function payload and return the operation result."""
        if not self._is_session_valid():
            self.login()

        response = self._request(self._format_payload(request_data))
        self.validate_response(response)
        parsed = self.parse_response(response)
        result = parsed["response"]["operation"]["result"]
        self.logger.info("Successful XML request with response: %s", result)
        return result

    def validate_response(self, response: requests.Response) -> None:
        try:
            parsed = self.parse_response(response)
            result = parsed.get("response", {})
            operation_result = result.get("operation", {}).get("result", {})
            status = operation_result.get("status", "")
            if status == "success":
                return

            error = operation_result.get("errormessage") or parsed.get(
                "errormessage", parsed
            )
            if response.status_code == 429 or 500 <= response.status_code < 600:
                raise RetriableAPIError(str(error), response)
            raise FatalAPIError(str(error))
        except (KeyError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise FatalAPIError(f"Failed to parse XML response: {exc!r}") from exc

    @staticmethod
    def parse_response(response: requests.Response) -> dict[str, Any]:
        return json.loads(json.dumps(xmltodict.parse(response.text)))

    def _login_request_body(self) -> dict[str, Any]:
        login_payload: dict[str, Any] = {
            "userid": self.config["user_id"],
            "companyid": self.config["company_id"],
            "password": self.config["user_password"],
        }
        if self._location_id:
            login_payload["locationid"] = self._location_id

        now = self._control_id()
        return {
            "request": {
                "control": self._control_block(now),
                "operation": {
                    "authentication": {"login": login_payload},
                    "content": {
                        "function": {
                            "@controlid": now,
                            "getAPISession": None,
                        }
                    },
                },
            }
        }

    def _format_payload(self, payload: dict[str, Any]) -> bytes:
        content: dict[str, Any] = {
            "function": {"@controlid": self._control_id()},
        }
        content["function"].update(payload)
        body = {
            "request": {
                "control": self._control_block(self._control_id()),
                "operation": {
                    "authentication": {"sessionid": self.session_id},
                    "content": content,
                },
            }
        }
        return xmltodict.unparse(body).encode("utf-8")

    def _control_block(self, control_id: str) -> dict[str, Any]:
        return {
            "senderid": self.config["sender_id"],
            "password": self.config["sender_password"],
            "controlid": control_id,
            "uniqueid": False,
            "dtdversion": 3.0,
            "includewhitespace": False,
        }

    def _is_session_valid(self) -> bool:
        if not self.session_id or not self.session_timeout_timestamp:
            return False
        now = round(dt.datetime.now(dt.timezone.utc).timestamp())
        return (self.session_timeout_timestamp - now) >= 120

    @staticmethod
    def _session_timeout(operation: dict[str, Any]) -> int:
        try:
            return round(
                dt.datetime.fromisoformat(
                    operation["authentication"]["sessiontimeout"]
                ).timestamp()
            )
        except (KeyError, ValueError, TypeError):
            return round(
                (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)).timestamp()
            )

    @staticmethod
    def _control_id() -> str:
        return str(dt.datetime.now(dt.timezone.utc).timestamp())

    def _redact(self, request_body: Any) -> str:
        text = str(request_body)
        for field in ("sender_id", "sender_password", "user_password", "user_id"):
            value = self.config.get(field)
            if value:
                text = text.replace(str(value), "REDACTED")
        if self.session_id:
            text = text.replace(self.session_id, "REDACTED")
        return text

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(self, request_data: bytes) -> requests.Response:
        self.logger.info(
            "Making XML request to %s with payload: %s",
            XML_BASE_URL,
            self._redact(request_data),
        )
        try:
            return requests.post(
                XML_BASE_URL,
                headers=self.http_headers,
                data=request_data,
                timeout=60,
            )
        except requests.RequestException as exc:
            raise FatalAPIError(f"XML HTTP request failed: {exc!r}") from exc


def extract_xml_record_id(result: dict[str, Any], object_name: str) -> str | None:
    """Pull RECORDNO / key from a successful XML create/update result."""
    if result.get("key") not in (None, ""):
        return str(result["key"])

    data = result.get("data") or {}
    if data.get("@key") not in (None, ""):
        return str(data["@key"])

    for key in (object_name.lower(), object_name.upper(), object_name):
        obj = data.get(key)
        if isinstance(obj, list) and obj:
            obj = obj[0]
        if isinstance(obj, dict) and obj.get("RECORDNO") not in (None, ""):
            return str(obj["RECORDNO"])

    return None
