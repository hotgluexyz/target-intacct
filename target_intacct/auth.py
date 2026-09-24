"""Authentication helpers for Intacct."""

from typing import Optional

from hotglue_singer_sdk.target_sdk.auth import OAuthAuthenticator


class IntacctAuthenticator(OAuthAuthenticator):
    """OAuth 2.0 refresh-token authenticator for Intacct.

    Always refreshes against Intacct's OAuth endpoint. Newer SDK versions
    default ``_refresh_token_via_hg_api`` to True, which hits Hotglue's
    /accesstoken API (unsupported for this connector).
    """

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
            auth_endpoint: Intacct OAuth token endpoint URL.
        """
        super().__init__(target, state, auth_endpoint=auth_endpoint)

    def update_access_token(self) -> None:
        """Refresh the access token via Intacct OAuth only."""
        self._update_access_token_locally()

    @property
    def oauth_request_body(self) -> dict:
        """OAuth request body for the refresh-token grant."""
        return {
            "refresh_token": self._config["refresh_token"],
            "grant_type": "refresh_token",
            "client_id": self._config["client_id"],
            "client_secret": self._config["client_secret"],
        }
