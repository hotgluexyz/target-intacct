import io
import json
import sys

import math
import pandas as pd

import singer

from .client import get_client
from .const import DEFAULT_API_URL, REQUIRED_CONFIG_KEYS

logger = singer.get_logger()


class DependencyException(Exception):
    pass


"""
Uploads Hours Per Week Denominator Statistical Journals to Intacct

Retrieves objects from Intacct API for verifying input data
Calls load_entries method
Sends entries for uploading to Intacct
"""


def hours_per_week_denominator_upload(intacct_client) -> None:
    logger.info("Starting upload.")

    # Load Active Classes, Customers, Accounts
    accounts = intacct_client.get_entity(
        object_type="general_ledger_accounts", fields=["RECORDNO", "ACCOUNTNO", "TITLE"]
    )
    classes = intacct_client.get_entity(
        object_type="classes", fields=["RECORDNO", "CLASSID", "NAME"]
    )
    customers = intacct_client.get_entity(
        object_type="customers", fields=["CUSTOMERID", "NAME"]
    )
    locations = intacct_client.get_entity(
        object_type="locations", fields=["LOCATIONID", "NAME"]
    )
    departments = intacct_client.get_entity(
        object_type="departments", fields=["DEPARTMENTID", "TITLE"]
    )

    journal_entries = load_hours_per_week_denominator_entries()

    # Post the journal entries to Intacct
    for je in journal_entries:
        intacct_client.post_journal(je)

    logger.info("Upload completed")


"""
Loads inputted data into Hours Per Week Denominator Statistical Journal Entries
"""


def load_hours_per_week_denominator_entries():
    # Get input from pipeline
    input_value = get_input()

    # Convert input from dictionary to DataFrame
    df = pd.DataFrame(input_value)
    # Verify it has required columns
    cols = list(df.columns)
    REQUIRED_COLS = [
        "whencreated",
        "locationid",
    ]

    if not all(col in cols for col in REQUIRED_COLS):
        logger.error(
            f"CSV is missing REQUIRED_COLS. Found={json.dumps(cols)}, Required={json.dumps(REQUIRED_COLS)}"
        )
        sys.exit(1)

    journal_entries = []
    errored = False

    def build_lines(x):
        logger.info(f"X value {x}...")

        line_items = []

        # Create line items
        for row in x.iterrows():
            # Create journal entry line detail
            je_detail = {
                "TRX_AMOUNT": str(round(float(row["Capacity"]), 2)),
                "TR_TYPE": 1,
            }

            # Get the Account Ref

            je_detail["ACCOUNTNO"] = 98051
            je_detail["EMPLOYEEID"] = row["employeeid"]
            je_detail["CLASSID"] = row["BusinessUnit"]
            je_detail["LOCATION"] = row["locationid"]
            je_detail["DEPARTMENT"] = row["PracticeAreaID"]
            # Create the line item
            line_items.append(je_detail)

        # Create the entry
        entry = {
            "JOURNAL": row.get("Journal", "STJ"),
            "BATCH_DATE": row["whencreated"],
            "BATCH_TITLE": "HOURS_PER_WEEK_DENOMINATOR",
            "ENTRIES": {"GLENTRY": line_items},
        }

        journal_entries.append(entry)

    # Build the entries
    df.groupby(lambda x: True).apply(build_lines)

    if errored:
        raise Exception("Building QBO JournalEntries failed!")

    # Print journal entries
    logger.info(f"Loaded {len(journal_entries)} journal entries to post")

    return journal_entries


"""
Uploads Financial Journals to Intacct

Retrieves objects from Intacct API for verifying input data
Calls load_entries method
Sends entries for uploading to Intacct
"""


def journal_upload(intacct_client, object_name) -> None:
    logger.info("Starting upload.")

    # Load Active Classes, Customers, Accounts
    accounts = intacct_client.get_entity(
        object_type="general_ledger_accounts", fields=["RECORDNO", "ACCOUNTNO", "TITLE"]
    )
    classes = intacct_client.get_entity(
        object_type="classes", fields=["RECORDNO", "CLASSID", "NAME"]
    )
    customers = intacct_client.get_entity(
        object_type="customers", fields=["CUSTOMERID", "NAME"]
    )
    locations = intacct_client.get_entity(
        object_type="locations", fields=["LOCATIONID", "NAME"]
    )
    departments = intacct_client.get_entity(
        object_type="departments", fields=["DEPARTMENTID", "TITLE"]
    )

    # Load Journal Entries CSV to post + Convert to Intacct format
    journal_entries = load_journal_entries(
        accounts, classes, customers, locations, departments, object_name
    )

    # Post the journal entries to Intacct
    for je in journal_entries:
        intacct_client.post_journal(je)

    logger.info("Upload completed")


"""
Loads inputted data into Financial Journal Entries
"""


def load_journal_entries(accounts, classes, locations, departments, object_name):
    # Get input from pipeline
    input_value = get_input()

    # Convert input from dictionary to DataFrame
    df = pd.DataFrame(input_value)
    # Verify it has required columns
    cols = list(df.columns)
    REQUIRED_COLS = [
        "Transaction Date",
        "Journal Entry Id",
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

    def build_lines(x):
        logger.info(f"X value {x}...")

        logger.info(f"Converting {object_name}...")
        line_items = []

        # Create line items
        for row in x.iterrows():
            # Create journal entry line detail
            je_detail = {
                "DESCRIPTION": row["Description"],
                "TRX_AMOUNT": str(round(float(row["Amount"]), 2)),
                "TR_TYPE": 1 if row["Posting Type"].upper() == "DEBIT" else -1,
            }

            # Get the Account Ref
            acct_num = (
                str(int(row["Account Number"]))
                if row["Account Number"] is not None
                and not math.isnan(int(row["Account Number"]))
                else None
            )
            acct_name = row["Account Name"]
            acct_ref = (
                acct_num
                if acct_num is not None
                else next(
                    (x["ACCOUNTNO"] for x in accounts if x["TITLE"] == acct_name), None
                )
            )

            if acct_ref is not None:
                je_detail["ACCOUNTNO"] = acct_ref
            else:
                errored = True
                logger.error(
                    f"Account is missing on Journal Entry {object_name}! Name={acct_name} No={acct_num}"
                )

            # Get the Class Ref
            class_name = row["Class"]
            class_ref = next(
                (x["CLASSID"] for x in classes if x["NAME"] == class_name), None
            )

            if class_ref is not None:
                je_detail["CLASSID"] = class_ref
            else:
                logger.warning(
                    f"Class is missing on Journal Entry {object_name}! Name={class_name}"
                )

            # Get the Location Ref if Location column exist
            if "Location" in row.index:
                location_name = row["Location"]
                location_ref = next(
                    (x["LOCATIONID"] for x in locations if x["NAME"] == location_name),
                    None,
                )

                if location_ref is not None:
                    je_detail["LOCATION"] = location_ref
                else:
                    logger.warning(
                        f"Location is missing on Journal Entry {object_name}! Name={location_name}"
                    )

            # Get the Department Ref if Department column exist
            if "Department" in row.index:
                department_name = row["Department"]
                department_ref = next(
                    (
                        x["DEPARTMENTID"]
                        for x in departments
                        if x["TITLE"] == department_name
                    ),
                    None,
                )

                if department_ref is not None:
                    je_detail["DEPARTMENT"] = department_ref
                else:
                    logger.warning(
                        f"Department is missing on Journal Entry {object_name}! Name={department_name}"
                    )

            # Append the currency if provided
            if row.get("Currency") is not None:
                je_detail["CURRENCY"] = row["Currency"]

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

    # Build the entries
    df.groupby("Journal Entry Id").apply(build_lines)

    if errored:
        raise Exception("Building QBO JournalEntries failed!")

    # Print journal entries
    logger.info(f"Loaded {len(journal_entries)} journal entries to post")

    return journal_entries


"""
Read the input from the pipeline and return a dictionary of the Records
"""


def get_input():
    input = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    input_value = {}

    # For each line of input, if it has data content (is a record) add the line to the dictionary
    for row in input:
        try:
            o = singer.parse_message(row).asdict()
            logger.info(f"INIT Input Value {o}")
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(row))
            raise
        message_type = o["type"]
        if message_type == "RECORD" and not any(
            value == "" or value is None for value in o["record"].values()
        ):
            if not input_value:
                input_value = o["record"]
                for key in o["record"].keys():
                    input_value[key] = [input_value[key]]
            else:
                for key in o["record"].keys():
                    input_value[key].append(o["record"][key])
    logger.info(f"Final Input Value {input_value}")
    return input_value


@singer.utils.handle_top_exception(logger)
def main() -> None:
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    # Login
    intacct_client = get_client(
        api_url=config.get("api_url", DEFAULT_API_URL),
        company_id=config["company_id"],
        sender_id=config["sender_id"],
        sender_password=config["sender_password"],
        user_id=config["user_id"],
        user_password=config["user_password"],
        headers={"User-Agent": config["user_agent"]} if "user_agent" in config else {},
    )

    object_name = config["object_name"]

    if object_name == "journal":
        journal_upload(intacct_client, object_name)
    elif object_name == "hours_per_week_denominator":
        hours_per_week_denominator_upload(intacct_client, object_name)


if __name__ == "__main__":
    main()
