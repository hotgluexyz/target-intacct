"""Ensure an open bank reconciliation exists for XML bank feeds."""

from __future__ import annotations

import calendar
import logging
from datetime import date
from typing import Any

from hotglue_singer_sdk.exceptions import FatalAPIError

from target_intacct.xml_client import IntacctXmlClient, extract_xml_record_id

OPEN_RECON_STATES = frozenset({"initiated", "draft"})
DEFAULT_RECON_MODE = "AutomatchReview"
DEFAULT_FEED_TYPE = "xml"
DEFAULT_STMT_ENDING_BALANCE = "0"

# Top-level "Simple Match Rule (Date, Amount, Doc#)" — fine for unrestricted accounts.
TOP_LEVEL_BANK_RULESET_KEY = "1"
# Entity-owned accounts cannot use top-level rulesets that include create rules.
ENTITY_BANK_RULESET_ID = "HG-MATCH"
ENTITY_BANK_RULESET_NAME = "Hotglue Bank Match"
# Shared match rule: "Simple Amount, Doc Num, and Date match"
DEFAULT_MATCH_RULE_KEY = "1"


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

    Sets ``client.location_id`` to the account's mega-entity (or top-level when the
    account is unrestricted). The caller should restore the prior location after the
    related bank-feed create.
    """
    cache_key = f"{financial_entity}|{mode}|{feed_type}"
    if cache is not None and cache_key in cache:
        return cache[cache_key]

    account = lookup_checking_account(client, financial_entity=financial_entity)
    if not account:
        message = (
            f"Checking account '{financial_entity}' was not found in Intacct. "
            "Use the Intacct BANKACCOUNTID as bankAccountId."
        )
        raise FatalAPIError(message)

    client.location_id = _session_location_for_account(account, logger=logger)
    ensure_checking_account_rule_set(
        client,
        financial_entity=financial_entity,
        account=account,
        logger=logger,
    )

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


def resolve_session_location_for_account(
    client: IntacctXmlClient,
    *,
    financial_entity: str,
    logger: logging.Logger,
) -> str | None:
    """Pick the XML login location for bank-feed / recon calls on this account."""
    account = lookup_checking_account(client, financial_entity=financial_entity)
    if not account:
        message = (
            f"Checking account '{financial_entity}' was not found in Intacct. "
            "Use the Intacct BANKACCOUNTID as bankAccountId."
        )
        raise FatalAPIError(message)
    return _session_location_for_account(account, logger=logger)


def _session_location_for_account(
    account: dict[str, Any],
    *,
    logger: logging.Logger,
) -> str | None:
    """Entity-owned accounts use MEGAENTITYID; unrestricted accounts use top-level."""
    mega_entity_id = account.get("MEGAENTITYID")
    logger.info(
        "Resolved checking account %s (RECORDNO=%s LOCATIONID=%s MEGAENTITYID=%s)",
        account.get("BANKACCOUNTID"),
        account.get("RECORDNO"),
        account.get("LOCATIONID"),
        mega_entity_id,
    )
    if mega_entity_id not in (None, ""):
        return str(mega_entity_id)
    return None


def ensure_checking_account_rule_set(
    client: IntacctXmlClient,
    *,
    financial_entity: str,
    logger: logging.Logger,
    account: dict[str, Any] | None = None,
) -> str:
    """Ensure the checking account has a bank-feed reconciliation rule set.

    Unrestricted accounts get top-level ruleset ``001`` (Simple Match Rule).
    Entity-owned accounts get/create ``HG-MATCH`` (match-only) with the shared
    simple date/amount/doc# match rule mapped in — top-level ``001`` cannot be
    assigned because it includes create rules owned at top-level.
    """
    account = account or lookup_checking_account(client, financial_entity=financial_entity)
    if not account:
        message = f"Checking account '{financial_entity}' was not found in Intacct."
        raise FatalAPIError(message)

    existing = account.get("RULESETKEY")
    if existing not in (None, ""):
        logger.info(
            "Checking account %s already has rule set key=%s id=%s",
            financial_entity,
            existing,
            account.get("RULESETID"),
        )
        return str(existing)

    previous_location = client.location_id
    try:
        client.location_id = _session_location_for_account(account, logger=logger)
        if client.location_id:
            ruleset_key = ensure_entity_bank_match_ruleset(client, logger=logger)
        else:
            ruleset_key = TOP_LEVEL_BANK_RULESET_KEY

        logger.info(
            "Assigning rule set key=%s to checking account %s",
            ruleset_key,
            financial_entity,
        )
        client.request_api(
            {
                "update": {
                    "CHECKINGACCOUNT": {
                        "BANKACCOUNTID": financial_entity,
                        "RULESETKEY": ruleset_key,
                    }
                }
            }
        )
        return ruleset_key
    finally:
        client.location_id = previous_location


def ensure_entity_bank_match_ruleset(
    client: IntacctXmlClient,
    *,
    logger: logging.Logger,
) -> str:
    """Return RECORDNO of entity-level HG-MATCH ruleset, creating/mapping if needed."""
    ruleset = _find_bank_ruleset_by_id(client, ENTITY_BANK_RULESET_ID)
    if not ruleset:
        logger.info("Creating entity bank match ruleset %s", ENTITY_BANK_RULESET_ID)
        result = client.request_api(
            {
                "create": {
                    "BANKTXNRULESET": {
                        "RULESETID": ENTITY_BANK_RULESET_ID,
                        "RULESETNAME": ENTITY_BANK_RULESET_NAME,
                        "ACCOUNTTYPE": "bank",
                        "RULESETTYPE": "match",
                        "STATUS": "active",
                    }
                }
            }
        )
        ruleset_key = extract_xml_record_id(result, "BANKTXNRULESET")
        if not ruleset_key:
            message = f"Failed to create bank ruleset {ENTITY_BANK_RULESET_ID}: {result}"
            raise FatalAPIError(message)
    else:
        ruleset_key = str(ruleset["RECORDNO"])

    _ensure_ruleset_has_match_rule(client, ruleset_key=ruleset_key, logger=logger)
    return ruleset_key


def _find_bank_ruleset_by_id(
    client: IntacctXmlClient,
    ruleset_id: str,
) -> dict[str, Any] | None:
    safe_id = str(ruleset_id).replace("'", "\\'")
    result = client.request_api(
        {
            "readByQuery": {
                "object": "BANKTXNRULESET",
                "fields": "RECORDNO,RULESETID,RULESETNAME,ACCOUNTTYPE,RULESETTYPE,STATUS",
                "query": f"RULESETID = '{safe_id}'",
                "pagesize": 1,
            }
        }
    )
    record = (result.get("data") or {}).get("banktxnruleset")
    if isinstance(record, list):
        return record[0] if record else None
    return record if isinstance(record, dict) else None


def _ensure_ruleset_has_match_rule(
    client: IntacctXmlClient,
    *,
    ruleset_key: str,
    logger: logging.Logger,
) -> None:
    result = client.request_api(
        {
            "read": {
                "object": "BANKTXNRULESET",
                "keys": ruleset_key,
                "fields": "*",
            }
        }
    )
    ruleset = ((result.get("data") or {}).get("BANKTXNRULESET")) or {}
    maps = (ruleset.get("BANKTXNRULEMAPS") or {}).get("BANKTXNRULEMAP") or []
    if isinstance(maps, dict):
        maps = [maps]
    if any(str(item.get("RULEKEY")) == DEFAULT_MATCH_RULE_KEY for item in maps):
        return

    logger.info(
        "Mapping match rule %s onto ruleset %s",
        DEFAULT_MATCH_RULE_KEY,
        ruleset_key,
    )
    client.request_api(
        {
            "update": {
                "BANKTXNRULESET": {
                    "RECORDNO": ruleset_key,
                    "BANKTXNRULEMAPS": {
                        "BANKTXNRULEMAP": {
                            "RULEKEY": DEFAULT_MATCH_RULE_KEY,
                            "RULEORDER": "1",
                        }
                    },
                }
            }
        }
    )


def lookup_checking_account(
    client: IntacctXmlClient,
    *,
    financial_entity: str,
) -> dict[str, Any] | None:
    """Look up a checking account by BANKACCOUNTID in the current, then top-level, session."""
    account = _query_checking_account(client, financial_entity)
    if account:
        return account

    previous = client.location_id
    if previous is None:
        return None

    try:
        client.location_id = None
        return _query_checking_account(client, financial_entity)
    finally:
        client.location_id = previous


def _query_checking_account(
    client: IntacctXmlClient,
    financial_entity: str,
) -> dict[str, Any] | None:
    safe_id = str(financial_entity).replace("'", "\\'")
    result = client.request_api(
        {
            "readByQuery": {
                "object": "CHECKINGACCOUNT",
                "fields": (
                    "BANKACCOUNTID,BANKNAME,LOCATIONID,RECORDNO,STATUS,"
                    "MEGAENTITYID,RULESETKEY,RULESETID"
                ),
                "query": f"BANKACCOUNTID = '{safe_id}'",
                "pagesize": 1,
            }
        }
    )
    account = (result.get("data") or {}).get("checkingaccount")
    if isinstance(account, list):
        return account[0] if account else None
    return account if isinstance(account, dict) else None


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
        "Creating BANKACCTRECON for %s mode=%s ending=%s balance=%s location=%s",
        financial_entity,
        mode,
        recon["STMTENDINGDATE"],
        recon["STMTENDINGBALANCE"],
        client.location_id or "TOP_LEVEL",
    )
    result = client.request_api({"create": {"BANKACCTRECON": recon}})
    recon_id = extract_xml_record_id(result, "BANKACCTRECON")
    if not recon_id:
        message = f"BANKACCTRECON create succeeded but no RECORDNO found: {result}"
        raise FatalAPIError(message)

    # Intacct can accept the create while silently ignoring MODE/FEEDTYPE (e.g. when
    # the account cannot use bank feeds). Verify so we don't treat a Manual recon as
    # AutomatchReview — Manual has no Bank tab and feeds stay unlinked.
    created = client.request_api(
        {
            "read": {
                "object": "BANKACCTRECON",
                "keys": recon_id,
                "fields": "RECORDNO,MODE,FEEDTYPE,STATE",
            }
        }
    )
    record = ((created.get("data") or {}).get("BANKACCTRECON")) or {}
    actual_mode = record.get("MODE")
    actual_feed_type = record.get("FEEDTYPE")
    if actual_mode != mode or actual_feed_type not in (feed_type, None, ""):
        # FEEDTYPE may be blank until the first feed lands; MODE must match.
        if actual_mode != mode:
            message = (
                f"Created BANKACCTRECON {recon_id} but MODE is '{actual_mode}' "
                f"(expected '{mode}'). Cancel this reconciliation and ensure the "
                "checking account has a bank-feed rule set before retrying."
            )
            raise FatalAPIError(message)

    logger.info(
        "Created BANKACCTRECON %s for %s (mode=%s feedType=%s)",
        recon_id,
        financial_entity,
        actual_mode,
        actual_feed_type,
    )
    return recon_id


def _parse_xml_date(value: str) -> date:
    text = value.strip()
    if len(text) >= 10 and text[2] == "/" and text[5] == "/":
        month, day, year = text[:10].split("/")
        return date(int(year), int(month), int(day))
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
