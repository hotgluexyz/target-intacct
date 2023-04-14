import json
import sys

import pandas as pd

import singer

from .utils import format_date_to_intacct, get_input, set_journal_entry_value

logger = singer.get_logger()

"""
Uploads Hours Per Week Denominator Statistical Journals to Intacct

Retrieves objects from Intacct API for verifying input data
Calls load_entries method
Sends entries for uploading to Intacct
"""


def hours_per_week_denominator_upload(intacct_client, object_name) -> None:
    logger.info("Starting upload.")

    # Load Current Data in Intacct for input verification
    employee_ids = intacct_client.get_entity(
        object_type="employees", fields=["EMPLOYEEID"]
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

    # Journal Entries to be uploaded
    journal_entries = load_hours_per_week_denominator_entries(
        employee_ids, business_units, location_ids, practice_area_ids, object_name
    )

    # Post the journal entries to Intacct
    for entry in journal_entries:
        intacct_client.post_journal(entry)

    logger.info("Upload completed")


"""
Loads inputted data into Hours Per Week Denominator Statistical Journal Entries
"""


def load_hours_per_week_denominator_entries(
    employee_ids, business_units, location_ids, practice_area_ids, object_name
):
    # Get input from pipeline
    input_value = get_input()

    # Convert input from dictionary to DataFrame
    data_frame = pd.DataFrame(input_value)

    # Verify it has required columns
    cols = list(data_frame.columns)
    REQUIRED_COLS = [
        "employeeid",
        "Capacity",
        "BudgetedBillable",
        "locationid",
        "PracticeAreaID",
        "BusinessUnit",
        "contact_name",
        "whencreated",
    ]

    if not all(col in cols for col in REQUIRED_COLS):
        logger.error(
            f"CSV is missing REQUIRED_COLS. Found={json.dumps(cols)}, Required={json.dumps(REQUIRED_COLS)}"
        )
        sys.exit(1)

    # Build the entries
    journal_entries, errored = build_lines(
        data_frame,
        employee_ids,
        business_units,
        location_ids,
        practice_area_ids,
        object_name,
    )
    # If an error occurred when loading entries
    if errored:
        raise Exception(
            "Building Hours Per Week Denominator Statistical Journal Entries failed!"
        )

    # Print journal entries
    logger.info(f"Loaded {len(journal_entries)} journal entries to post")

    return journal_entries


def build_lines(
    data, employee_ids, business_units, location_ids, practice_area_ids, object_name
):
    logger.info(f"Converting {object_name}...")
    line_items = []
    journal_entries = []
    errored = False

    # Create line items
    for index, row in data.iterrows():
        employee_id = row["employeeid"]
        capacity = row["Capacity"]
        business_unit = row["BusinessUnit"]
        location_id = row["locationid"]
        practice_area_id = row["PracticeAreaID"]

        # Create journal entry line detail
        je_detail = {
            "AMOUNT": str(round(float(capacity), 2)),
            "TR_TYPE": 1,
            "ACCOUNTNO": 98051,
        }

        entry_error = False
        for lst, field, to_search in [
            (employee_ids, "EMPLOYEEID", employee_id),
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
        "JOURNAL": row.get("Journal", "STJ"),
        "BATCH_DATE": format_date_to_intacct(row["whencreated"]),
        "BATCH_TITLE": object_name.upper(),
        "ENTRIES": {"GLENTRY": line_items},
    }

    journal_entries.append(entry)

    return journal_entries, errored
