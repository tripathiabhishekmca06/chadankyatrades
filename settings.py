"""Load API keys from environment variables or Streamlit secrets (Community Cloud).

Priority: ``os.environ`` first, then ``st.secrets`` (TOML from Cloud UI or local
``.streamlit/secrets.toml``). Never commit real keys; use Cloud **Secrets** or a
local ``secrets.toml`` (gitignored).
"""

from __future__ import annotations

import os


def _streamlit_secret(key: str) -> str:
    try:
        import streamlit as st

        if key in st.secrets:
            return str(st.secrets[key]).strip()
    except Exception:
        pass
    return ""


def get_alpha_vantage_api_key() -> str:
    for env_name in ("ALPHA_VANTAGE_API_KEY", "ALPHAVANTAGE_API_KEY"):
        v = os.getenv(env_name, "").strip()
        if v:
            return v
    return _streamlit_secret("ALPHA_VANTAGE_API_KEY")


def get_eodhd_api_key() -> str:
    v = os.getenv("EODHD_API_KEY", "").strip()
    if v:
        return v
    return _streamlit_secret("EODHD_API_KEY")
