"""Authentication helpers for Intacct."""

from typing import Optional

from hotglue_singer_sdk.target_sdk.auth import OAuthAuthenticator


class IntacctAuthenticator(OAuthAuthenticator):
    """OAuth 2.0 refresh-token authenticator for Intacct."""

    def __init__(
        self,
        target,
        state,
        auth_endpoint: Optional[str] = None,
    ) -> None:
        """Initialize the authenticator.

        Args:
            target: The Singer target instance.
            state: Authentication state.
            auth_endpoint: Token endpoint URL (from ``target.access_token_support``).
        """
        super().__init__(target, state, auth_endpoint=auth_endpoint)

    @property
    def oauth_request_body(self) -> dict:
        """OAuth request body for the refresh-token grant."""
        return {
            "refresh_token": self._config["refresh_token"],
            "grant_type": "refresh_token",
            "client_id": self._config["client_id"],
            "client_secret": self._config["client_secret"],
        }