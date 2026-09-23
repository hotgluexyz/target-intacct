"""Tests for Intacct error message parsing."""

from __future__ import annotations

from target_intacct.client import parse_intacct_error_message


def test_parse_intacct_error_prefers_unique_detail_messages() -> None:
    payload = {
        "ia::result": {
            "ia::error": {
                "code": "operationFailed",
                "message": (
                    "POST request on objects/cash-management/checking-account "
                    "object was unsuccessful"
                ),
                "details": [
                    {"errorId": "CM-0102", "message": "Could not create a bank account."},
                    {"errorId": "CM-0102", "message": "Could not create a bank account."},
                    {
                        "errorId": "CM-0160",
                        "message": "GL account '10020' is associated with cash account '200_SVB'.",
                    },
                ],
            }
        },
        "ia::meta": {"totalCount": 1, "totalSuccess": 0, "totalError": 1},
    }

    assert parse_intacct_error_message(payload) == (
        "Could not create a bank account.; "
        "GL account '10020' is associated with cash account '200_SVB'."
    )


def test_parse_intacct_error_falls_back_to_top_level_message() -> None:
    payload = {
        "ia::result": {
            "ia::error": {
                "code": "operationFailed",
                "message": "POST request failed",
                "details": [],
            }
        }
    }

    assert parse_intacct_error_message(payload) == "POST request failed"


def test_parse_intacct_error_returns_none_for_unknown_shape() -> None:
    assert parse_intacct_error_message({"foo": "bar"}) is None
    assert parse_intacct_error_message("not-json") is None
