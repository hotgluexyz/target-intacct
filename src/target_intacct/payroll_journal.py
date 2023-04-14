import json
import sys

import pandas as pd

import singer

from .utils import get_input, set_journal_entry_value

logger = singer.get_logger()

"""
Uploads Financial Journals to Intacct

Retrieves objects from Intacct API for verifying input data
Calls load_entries method
Sends entries for uploading to Intacct
"""


def journal_upload(intacct_client, object_name) -> None:
    logger.info("Starting upload.")

    # Load Active Classes, Customers, Accounts
    account_ids = intacct_client.get_entity(
        object_type="general_ledger_accounts", fields=["ACCOUNTNO"]
    )
    business_units = intacct_client.get_entity(
        object_type="classes", fields=["CLASSID"]
    )
    location_ids = intacct_client.get_entity(
        object_type="locations", fields=["LOCATIONID"]
    )
    practice_area_ids = intacct_client.get_entity(
        object_type="departments", fields=["DEPARTMENTID"]
    )

    # Load Journal Entries CSV to post + Convert to Intacct format
    journal_entries = load_journal_entries(
        account_ids,
        business_units,
        location_ids,
        practice_area_ids,
        object_name,
    )

    # Post the journal entries to Intacct
    for je in journal_entries:
        intacct_client.post_journal(je)

    logger.info("Upload completed")


"""
Loads inputted data into Financial Journal Entries
"""


def load_journal_entries(
    account_ids,
    business_units,
    location_ids,
    practice_area_ids,
    object_name,
):
    # Get input from pipeline
    input_value = get_input()

    # Convert input from dictionary to DataFrame
    data_frame = pd.DataFrame(input_value)
    # Verify it has required columns
    cols = list(data_frame.columns)
    REQUIRED_COLS = [
        "Transaction Date",
        "Class",
        "Account Number",
        "Account Name",
        "Posting Type",
        "Description",
    ]

    if not all(col in cols for col in REQUIRED_COLS):
        logger.error(
            f"CSV is missing REQUIRED_COLS. Found={json.dumps(cols)}, Required={json.dumps(REQUIRED_COLS)}"
        )
        sys.exit(1)

    journal_entries = []
    errored = False

    # Build the entries
    journal_entries, errored = build_lines(
        data_frame,
        account_ids,
        business_units,
        location_ids,
        practice_area_ids,
        object_name,
    )

    if errored:
        raise Exception("Building Payroll Journal Entries failed!")

    # Print journal entries
    logger.info(f"Loaded {len(journal_entries)} journal entries to post")

    return journal_entries


def build_lines(
    data,
    account_ids,
    business_units,
    location_ids,
    practice_area_ids,
    object_name,
):
    logger.info(f"Converting {object_name}...")
    line_items = []
    journal_entries = []
    errored = False

    # Create line items
    for index, row in data.iterrows():
        account_id = row["AccountNumber"]
        business_unit = row["BusinessUnit"]
        location_id = row["locationid"]
        practice_area_id = row["PracticeAreaID"]
        currency = row["Currency"]
        description = row["Description"]
        amount = row["Amount"]
        tr_type = row["TR_TYPE"]
        exchange_rate = row["ExchangeRate"]

        # Create journal entry line detail
        je_detail = {
            "DESCRIPTION": description,
            "TRX_AMOUNT": str(round(float(amount), 2)),
            "TR_TYPE": 1 if tr_type.upper() == "DEBIT" else -1,
            "CURRENCY": currency,
            "EXCH_RATE_TYPE_ID": exchange_rate,
        }

        entry_error = False
        for lst, field, to_search in [
            (account_ids, "ACCOUNTNO", account_id),
            (business_units, "CLASSID", business_unit),
            (location_ids, "LOCATIONID", location_id),
            (practice_area_ids, "DEPARTMENTID", practice_area_id),
        ]:
            entry_error = set_journal_entry_value(
                je_detail, lst, field, to_search, object_name
            )
            if entry_error:
                break

        if entry_error:
            errored = True

        # Create the line item
        line_items.append(je_detail)

    # Create the entry
    entry = {
        "JOURNAL": row.get("Journal", "PYRJ"),
        "BATCH_DATE": row["Transaction Date"],
        "REVERSEDDATE": row["Transaction Date"],
        "BATCH_TITLE": object_name,
        "ENTRIES": {"GLENTRY": line_items},
    }

    journal_entries.append(entry)

    return journal_entries, errored
