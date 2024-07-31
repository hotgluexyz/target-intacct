#!/usr/bin/env python3
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Dict, FrozenSet, Optional

import math
import pandas as pd

import singer
from singer import metadata

from .client import SageIntacctSDK, get_client
from .const import DEFAULT_API_URL, KEY_PROPERTIES, REQUIRED_CONFIG_KEYS

logger = singer.get_logger()


class DependencyException(Exception):
    pass


def _get_abs_path(path: str) -> Path:
    p = Path(__file__).parent / path
    return p.resolve()


def _get_start(key: str) -> dt.datetime:
    if key in Context.state:
        # Subtract look-back from Config (default 1 hour) from State, in case of late arriving records.
        start = singer.utils.strptime_to_utc(Context.state[key]) - dt.timedelta(
            hours=Context.config['event_lookback']
        )
    else:
        start = singer.utils.strptime_to_utc(Context.config['start_date'])

    return start


def load_journal_entries(config, accounts, classes, customers, locations, departments):
    # Get input path
    input_path = f"{config['input_path']}/JournalEntries.csv"
    # Read the passed CSV
    df = pd.read_csv(input_path)
    # Verify it has required columns
    cols = list(df.columns)
    REQUIRED_COLS = ["Transaction Date", "Journal Entry Id", "Class", "Account Number", "Account Name", "Posting Type", "Description"]

    if not all(col in cols for col in REQUIRED_COLS):
        logger.error(f"CSV is missing REQUIRED_COLS. Found={json.dumps(cols)}, Required={json.dumps(REQUIRED_COLS)}")
        sys.exit(1)

    journal_entries = []
    errored = False

    def build_lines(x):
        # Get the journal entry id
        je_id = x['Journal Entry Id'].iloc[0]
        logger.info(f"Converting {je_id}...")
        line_items = []

        # Create line items
        for index, row in x.iterrows():
            # Create journal entry line detail
            je_detail = {
                "DESCRIPTION": row['Description'],
                "TRX_AMOUNT": str(round(row['Amount'], 2)),
                "TR_TYPE": 1 if row['Posting Type'].upper() == "DEBIT" else -1
            }

            # Get the Account Ref
            acct_num = str(int(row['Account Number'])) if row['Account Number'] is not None and not math.isnan(row['Account Number']) else None
            acct_name = row['Account Name']
            acct_ref = acct_num if acct_num is not None else next((x['ACCOUNTNO'] for x in accounts if x['TITLE'] == acct_name), None)

            if acct_ref is not None:
                je_detail["ACCOUNTNO"] = acct_ref
            else:
                errored = True
                logger.error(f"Account is missing on Journal Entry {je_id}! Name={acct_name} No={acct_num}")

            # Get the Class Ref
            class_name = row['Class']
            class_ref = next((x['CLASSID'] for x in classes if x['NAME'] == class_name), None)

            if class_ref is not None:
                je_detail["CLASSID"] = class_ref
            else:
                logger.warning(f"Class is missing on Journal Entry {je_id}! Name={class_name}")

            # Get the Location Ref if Location column exist
            if 'Location' in row.index:
                location_name = row['Location']
                location_ref = next((x['LOCATIONID'] for x in locations if x['NAME'] == location_name), None)

                if location_ref is not None:
                    je_detail["LOCATION"] = location_ref
                else:
                    logger.warning(f"Location is missing on Journal Entry {je_id}! Name={location_name}")

            # Get the Department Ref if Department column exist
            if 'Department' in row.index:
                department_name = row['Department']
                department_ref = next((x['DEPARTMENTID'] for x in departments if x['TITLE'] == department_name), None)

                if department_ref is not None:
                    je_detail["DEPARTMENT"] = department_ref
                else:
                    logger.warning(f"Department is missing on Journal Entry {je_id}! Name={department_name}")

            # Get the Quickbooks Customer
            if 'Customer ID' in row.index:
                customer_id = row['Customer ID']
                if customer_id is not None:
                    je_detail["CUSTOMERID"] = customer_id
                else:
                    logger.warning(f"Customer ID is missing on Journal Entry {je_id}! Name={customer_id}")
            elif 'Customer Name' in row.index:
                customer_name = row['Customer Name']
                customer_ref = next((x['CUSTOMERID'] for x in customers if x['NAME'] == customer_name), None)
                if customer_ref is not None:
                    je_detail["CUSTOMERID"] = customer_ref
                else:
                    logger.warning(f"Customer is missing on Journal Entry {je_id}! Name={customer_name}")

            # Append the currency if provided
            if row.get('Currency') is not None:
                je_detail['CURRENCY'] = row['Currency']

            # Support dynamic custom fields on Journal Entry Line level
            custom_fields = config.get("custom_fields") or []

            for ce in custom_fields:
                value = row.get(ce.get("input_id"))
                intacct_id = ce.get("intacct_id")
                if not pd.isna(value):
                    je_detail[intacct_id] = value

            # Create the line item
            line_items.append(je_detail)

        # Create the entry
        entry = {
            'JOURNAL': row.get('Journal', 'APJ'),
            'BATCH_DATE': row['Transaction Date'],
            'BATCH_TITLE': je_id,
            'ENTRIES': {
                'GLENTRY': line_items
            }
        }

        # Support dynamic custom fields on Journal Entry root level
        custom_fields = config.get("custom_fields") or []

        for ce in custom_fields:
            value = row.get(ce.get("input_id"))
            intacct_id = ce.get("intacct_id")
            if not pd.isna(value):
                entry[intacct_id] = value

        journal_entries.append(entry)

    # Build the entries
    df.groupby("Journal Entry Id").apply(build_lines)

    if errored:
        raise Exception("Building QBO JournalEntries failed!")

    # Print journal entries
    logger.info(f"Loaded {len(journal_entries)} journal entries to post")

    return journal_entries


def upload(config, intacct_client) -> None:
    """
    Syncs all streams selected in Context.catalog.
    Writes out state file for events stream once sync completed.
    """
    logger.info('Starting upload.')

    # Load Active Classes, Customers, Accounts
    accounts = intacct_client.get_entity(object_type="general_ledger_accounts", fields=["RECORDNO", "ACCOUNTNO", "TITLE"])
    classes = intacct_client.get_entity(object_type="classes", fields=["RECORDNO", "CLASSID", "NAME"])
    customers = intacct_client.get_entity(object_type="customers", fields=["CUSTOMERID", "NAME"])
    locations = intacct_client.get_entity(object_type="locations", fields=["LOCATIONID", "NAME"])
    departments = intacct_client.get_entity(object_type="departments", fields=["DEPARTMENTID", "TITLE"])

    # Load Journal Entries CSV to post + Convert to Intacct format
    journal_entries = load_journal_entries(config, accounts, classes, customers, locations, departments)

    # Post the journal entries to Intacct
    for je in journal_entries:
        intacct_client.post_journal(je)

    logger.info('Upload completed')


@singer.utils.handle_top_exception(logger)
def main() -> None:
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    # Login
    intacct_client = get_client(
        api_url=config.get('api_url', DEFAULT_API_URL),
        company_id=config['company_id'],
        sender_id=config['sender_id'],
        sender_password=config['sender_password'],
        user_id=config['user_id'],
        user_password=config['user_password'],
        headers={'User-Agent': config['user_agent']} if 'user_agent' in config else {},
    )

    # Upload the data
    upload(config, intacct_client)


if __name__ == '__main__':
    main()
