"""Ensure an open bank reconciliation exists for XML bank feeds."""

from __future__ import annotations

import calendar
import logging
from datetime import date, datetime
from typing import Any

from hotglue_singer_sdk.exceptions import FatalAPIError

from target_intacct.xml_client import IntacctXmlClient, extract_xml_record_id

OPEN_RECON_STATES = frozenset({"initiated", "draft"})
DEFAULT_RECON_MODE = "AutomatchReview"
DEFAULT_FEED_TYPE = "xml"
DEFAULT_STMT_ENDING_BALANCE = "0"


def ensure_open_bank_reconciliation(
    client: IntacctXmlClient,
    *,
    financial_entity: str,
    feed_date: str,
    logger: logging.Logger,
    mode: str = DEFAULT_RECON_MODE,
    feed_type: str = DEFAULT_FEED_TYPE,
    stmt_ending_date: str | None = None,
    cutoff_date: str | None = None,
    stmt_ending_balance: str | None = None,
    cache: dict[str, str] | None = None,
) -> str:
    """Return RECORDNO of an open AutomatchReview recon, creating one if needed.

    BANKACCTRECON create must run against a top-level XML session; Intacct rejects
    the same payload when logged into an entity location.
    """
    cache_key = f"{financial_entity}|{mode}|{feed_type}"
    if cache is not None and cache_key in cache:
        return cache[cache_key]

    previous_location = client.location_id
    try:
        client.location_id = None
        existing = find_open_bank_reconciliation(
            client,
            financial_entity=financial_entity,
            mode=mode,
            feed_type=feed_type,
        )
        if existing:
            logger.info(
                "Using open BANKACCTRECON %s for %s (state=%s)",
                existing["RECORDNO"],
                financial_entity,
                existing.get("STATE"),
            )
            recon_id = str(existing["RECORDNO"])
            if cache is not None:
                cache[cache_key] = recon_id
            return recon_id

        recon_id = create_bank_reconciliation(
            client,
            financial_entity=financial_entity,
            feed_date=feed_date,
            mode=mode,
            feed_type=feed_type,
            stmt_ending_date=stmt_ending_date,
            cutoff_date=cutoff_date,
            stmt_ending_balance=stmt_ending_balance,
            logger=logger,
        )
        if cache is not None:
            cache[cache_key] = recon_id
        return recon_id
    finally:
        client.location_id = previous_location


def find_open_bank_reconciliation(
    client: IntacctXmlClient,
    *,
    financial_entity: str,
    mode: str = DEFAULT_RECON_MODE,
    feed_type: str = DEFAULT_FEED_TYPE,
) -> dict[str, Any] | None:
    """Query for an open reconciliation matching account / mode / feed type."""
    result = client.request_api(
        {
            "query": {
                "object": "BANKACCTRECON",
                "select": {
                    "field": [
                        "RECORDNO",
                        "FINANCIALENTITY",
                        "STATE",
                        "MODE",
                        "FEEDTYPE",
                        "STMTENDINGDATE",
                    ]
                },
                "filter": {
                    "equalto": {
                        "field": "FINANCIALENTITY",
                        "value": financial_entity,
                    }
                },
                "pagesize": 100,
                "options": {"showprivate": "true"},
            }
        }
    )

    records = (result.get("data") or {}).get("BANKACCTRECON") or []
    if isinstance(records, dict):
        records = [records]

    for record in records:
        state = str(record.get("STATE") or "").lower()
        if state not in OPEN_RECON_STATES:
            continue
        if str(record.get("MODE") or "") != mode:
            continue
        record_feed_type = record.get("FEEDTYPE")
        if record_feed_type not in (None, "", feed_type):
            continue
        return record

    return None


def create_bank_reconciliation(
    client: IntacctXmlClient,
    *,
    financial_entity: str,
    feed_date: str,
    mode: str = DEFAULT_RECON_MODE,
    feed_type: str = DEFAULT_FEED_TYPE,
    stmt_ending_date: str | None = None,
    cutoff_date: str | None = None,
    stmt_ending_balance: str | None = None,
    logger: logging.Logger,
) -> str:
    """Create a BANKACCTRECON and return its RECORDNO."""
    ending_date = stmt_ending_date or _default_stmt_ending_date(feed_date)
    recon: dict[str, Any] = {
        "FINANCIALENTITY": financial_entity,
        "STMTENDINGDATE": ending_date,
        "STMTENDINGBALANCE": (
            DEFAULT_STMT_ENDING_BALANCE
            if stmt_ending_balance in (None, "")
            else str(stmt_ending_balance)
        ),
        "MODE": mode,
        "FEEDTYPE": feed_type,
    }
    resolved_cutoff = cutoff_date or _default_cutoff_date(feed_date)
    if resolved_cutoff:
        recon["CUTOFFDATE"] = resolved_cutoff

    logger.info(
        "Creating BANKACCTRECON for %s mode=%s ending=%s balance=%s",
        financial_entity,
        mode,
        recon["STMTENDINGDATE"],
        recon["STMTENDINGBALANCE"],
    )
    result = client.request_api({"create": {"BANKACCTRECON": recon}})
    recon_id = extract_xml_record_id(result, "BANKACCTRECON")
    if not recon_id:
        message = f"BANKACCTRECON create succeeded but no RECORDNO found: {result}"
        raise FatalAPIError(message)
    logger.info("Created BANKACCTRECON %s for %s", recon_id, financial_entity)
    return recon_id


def _parse_xml_date(value: str) -> date:
    text = value.strip()
    if len(text) >= 10 and text[2] == "/" and text[5] == "/":
        return datetime.strptime(text[:10], "%m/%d/%Y").date()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return date.fromisoformat(text[:10])
    raise ValueError(f"Unrecognized date '{value}'")


def _default_stmt_ending_date(feed_date: str) -> str:
    """Use month-end of the feed date as the statement ending date."""
    parsed = _parse_xml_date(feed_date)
    last_day = calendar.monthrange(parsed.year, parsed.month)[1]
    return date(parsed.year, parsed.month, last_day).strftime("%m/%d/%Y")


def _default_cutoff_date(feed_date: str) -> str:
    """Use the first day of the feed-date month (needed for first recon)."""
    parsed = _parse_xml_date(feed_date)
    return date(parsed.year, parsed.month, 1).strftime("%m/%d/%Y")
