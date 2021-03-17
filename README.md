# target-intacct

This is a [hotglue](https://hotglue.xyz) target that sends CSV data back to Sage Intacct.

## Quick Start

1. Install

    ```bash
    pip install git+https://github.com/hotgluexyz/target-intacct.git
    ```

2. Create the config file

   Create a JSON file called `config.json`. Its contents should look like:

   ```json
    {
        "start_date": "2010-01-01",
        "company_id": "<Intacct Company Id>",
        "sender_id": "<Intacct Sender Id>",
        "sender_password": "<Intacct Sender Password>",
        "user_id": "<Intacct User Id>",
        "user_password": "<Intacct User Password>",
        "input_path": "<directory with CSV files to upload>"
    }
    ```

   The `start_date` specifies the date at which the tap will begin pulling data
   (for those resources that support this).

   The `company_id` is the Sage Intacct Company Id.

   The `sender_id` is the Sage Intacct Sender Id.

   The `sender_password` is the Sage Intacct Sender Password.

   The `user_id` is the Sage Intacct User Id.

   The `user_password` is the Sage Intacct User Password.

3. Run the Target

    ```bash
    target-intacct --config config.json
    ```
