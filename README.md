# target-intacct

`target-intacct` is a Singer target for **Intacct**, built with the [Hotglue Singer SDK](https://github.com/hotgluexyz/HotglueSingerSDK) (`TargetHotglue`, HTTP sinks, SDK authenticators).

This project was generated from the Hotglue target cookiecutter with:

| Option | Value |
|--------|--------|
| `auth_method` | `OAuth2` |
| `sinks` | `BankAccount | record, CreditCardAccount | record, BankTransaction | record, CreditCardTransaction | record` |
| `base_url` | `https://api.intacct.com/ia/api/v1` |

## Project layout

| Path | Role |
|------|------|
| `target_intacct/target.py` | `TargetIntacct` — config schema, `SINK_TYPES`, optional `default_sink_class` and `access_token_support` |
| `target_intacct/client.py` | `IntacctSink` base class, `RecordSink` / `BatchSink` stubs, `authenticator` |
| `target_intacct/sinks.py` | Per-stream sink classes (`FallbackSink` or named `*Sink` classes) |
| `target_intacct/auth.py` | Present only when generated with **OAuth2** (removed by cookiecutter hook otherwise) |
| `.secrets/config.json` | Sample config matching your `auth_method` |

## Configuration

Settings are defined in `config_jsonschema` on `TargetIntacct` in `target_intacct/target.py`.
For this project (**OAuth2**), config includes: `client_id`, `client_secret`, `refresh_token`, and optional `access_token` / `expires_in`.

Inspect the live schema:

```bash
target-intacct --about
target-intacct --about --format=markdown
```

Local secrets: edit `.secrets/config.json` (shape matches `auth_method` above).

## Authentication

| `auth_method` | Implementation |
|---------------|----------------|
| **OAuth2** | `IntacctAuthenticator` in `auth.py`; `TargetIntacct.access_token_support()` returns the authenticator class and token URL (replace the TODO endpoint in `target.py`). |
| **Bearer Token** | `BearerTokenAuthenticator` in `client.py` |
| **Basic Auth** | `BasicAuthenticator` with `username` / `password` |
| **API Key** | `ApiAuthenticator` with configurable header name/prefix |

HTTP calls use `base_url` on `IntacctSink` (`https://api.intacct.com/ia/api/v1` unless you change it in `client.py`).

## Sinks

Sinks are declared at scaffold time as `StreamName | record` or `StreamName | batch`, comma-separated.

Generated with: `BankAccount | record, CreditCardAccount | record, BankTransaction | record, CreditCardTransaction | record`
**Named sinks:** `sinks.py` defines one class per stream; `target.py` registers them in `SINK_TYPES`. Implement `endpoint` (and request logic) per sink in `sinks.py` / overrides on `client.py` bases.

Shared HTTP behavior lives in `client.py` (`IntacctRecordSink` / `IntacctBatchSink` as generated).

## Usage

```bash
target-intacct --version
target-intacct --help
tap-smoke-test | target-intacct --config /path/to/config.json
```

## Developer setup

Prerequisites: Python 3.10+, [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pytest
uv run target-intacct --help
tox -e lint    # ruff check + format
```

For implementation notes and conventions, see `AGENTS.md` (or `CLAUDE.md` if that was selected at generation).

## References

- [Hotglue Singer SDK](https://github.com/hotgluexyz/HotglueSingerSDK)
