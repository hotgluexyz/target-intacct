import singer

from .client import get_client
from .const import DEFAULT_API_URL, REQUIRED_CONFIG_KEYS
from .statistical_journal import statistical_journal_upload
from .payroll_journal import journal_upload

logger = singer.get_logger()


class DependencyException(Exception):
    pass


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
        entity_id=config["entity_id"],
    )

    object_name = config["object_name"]

    if object_name == "payroll_journal":
        journal_upload(intacct_client, object_name)
    elif object_name == "statistical_journal":
        statistical_journal_upload(intacct_client, object_name)
    else:
        raise Exception("Valid Object Name Not Found")


if __name__ == "__main__":
    main()
