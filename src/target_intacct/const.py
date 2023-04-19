REQUIRED_CONFIG_KEYS = [
    "company_id",
    "sender_id",
    "sender_password",
    "user_id",
    "user_password",
    "object_name",
    "entity_id",
]

# List of available objects with their internal object-reference/endpoint name.
INTACCT_OBJECTS = {
    "accounts_payable_bills": "APBILL",
    "accounts_payable_vendors": "VENDOR",
    "general_ledger_accounts": "GLACCOUNT",
    "general_ledger_details": "GLDETAIL",
    "general_ledger_journal_entries": "GLBATCH",
    "general_ledger_journal_entry_lines": "GLENTRY",
    "projects": "PROJECT",
    "customers": "CUSTOMER",
    "classes": "CLASS",
    "locations": "LOCATION",
    "departments": "DEPARTMENT",
    "employees": "EMPLOYEE",
}

DEFAULT_API_URL = "https://api.intacct.com/ia/xml/xmlgw.phtml"
