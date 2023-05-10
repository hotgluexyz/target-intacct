from datetime import datetime
import io
import sys
import json
from typing import Dict, List

import singer

logger = singer.get_logger()


def get_input():
    """Read the input from the pipeline and return a dictionary of the Records."""
    input = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    input_value = {}

    # For each line of input, if it has data content (is a record) add the line to the dictionary
    for row in input:
        try:
            raw_input = singer.parse_message(row).asdict()
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(row))
            raise
        message_type = raw_input["type"]
        if message_type == "RECORD" and not any(
            value == "" or value is None for value in raw_input["record"].values()
        ):
            record = raw_input["record"]
            if not input_value:
                input_value = {key: [value] for key, value in record.items()}
            else:
                for key, value in record.items():
                    input_value[key].append(value)
    return input_value


def set_journal_entry_value(
    je_detail: dict,
    intacct_values: List[Dict],
    field_name: str,
    search_value,
    object_name: str,
) -> bool:
    """Creates journal entries for statistical and financial journals."""
    errored = False
    if search_value and any(
        filter(lambda o: o.get(field_name) == str(search_value), intacct_values)
    ):
        je_field_name = (
            field_name
            if field_name in ["EMPLOYEEID", "CLASSID"]
            else field_name.replace("ID", "")
        )
        je_detail[je_field_name] = search_value
    else:
        errored = True
        logger.error(f"Field {field_name} is missing in Intacct {object_name}")
    return errored
