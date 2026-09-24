"""Intacct target class."""


from __future__ import annotations

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.target_sdk.target import TargetHotglue
from target_intacct.client import IntacctRecordSink
from target_intacct.sinks import BankAccountSink, CreditCardAccountSink, BankTransactionSink, CreditCardTransactionSink


class TargetIntacct(TargetHotglue):
    """Target for Intacct."""

    name = "target-intacct"
    SINK_TYPES = [BankAccountSink, CreditCardAccountSink, BankTransactionSink, CreditCardTransactionSink]

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType(),
            required=True,
            description="Client identifier for the token endpoint",
        ),
        th.Property(
            "client_secret",
            th.StringType(),
            required=True,
            description="Client secret for the token endpoint",
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=True,
            description="Refresh token used to obtain new access tokens",
        ),
        th.Property(
            "access_token",
            th.StringType,
            required=False,
            description="Current access token (usually populated after refresh)",
        ),
        th.Property(
            "expires_in",
            th.IntegerType,
            required=False,
            description="Epoch seconds when the access token expires (updated on refresh)",
        ),
        th.Property(
            "entity_id",
            th.StringType,
            required=False,
            description=(
                "Intacct entity ID sent on REST requests via the "
                "X-IA-API-Param-Entity header, and used as locationid for XML login"
            ),
        ),
        th.Property(
            "company_id",
            th.StringType,
            required=False,
            description="Company ID for the Intacct XML Web Services login",
        ),
        th.Property(
            "sender_id",
            th.StringType,
            required=False,
            description="Web Services sender ID for the Intacct XML API",
        ),
        th.Property(
            "sender_password",
            th.StringType,
            required=False,
            description="Web Services sender password for the Intacct XML API",
        ),
        th.Property(
            "user_id",
            th.StringType,
            required=False,
            description="User ID for the Intacct XML Web Services login",
        ),
        th.Property(
            "user_password",
            th.StringType,
            required=False,
            description="User password for the Intacct XML Web Services login",
        ),
    ).to_dict()


if __name__ == "__main__":
    TargetIntacct.cli()

