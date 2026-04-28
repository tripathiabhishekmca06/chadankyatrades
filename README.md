# Futures Trading Dashboard

Streamlit app for scanning an NSE-style FNO symbol list, generating technical trade signals, and tracking active and closed trades in SQLite.

## Repository layout

| Path | Purpose |
|------|---------|
| `app.py` | Main Streamlit entrypoint (set this as **Main file path** on Cloud) |
| `settings.py` | API keys from `os.environ` or `st.secrets` (no secrets in repo) |
| `data/fno_list.csv` | Universe CSV (`symbol` column); required for scans |
| `data/options_list.csv` | Optional options universe (omit file if unused) |
| `requirements.txt` | Python dependencies for Cloud and local installs |
| `runtime.txt` | Python version for [Streamlit Community Cloud](https://share.streamlit.io) |
| `.streamlit/config.toml` | Portable Streamlit settings (no host or port overrides) |
| `.streamlit/secrets.toml.example` | Template for local `secrets.toml` (copy and fill; do not commit real keys) |
| `.gitignore` | Local venv, DB, logs, cache, secrets |

Runtime SQLite (`signals.db`) and logs are created next to `app.py` on first run and are not committed.

## API keys (Alpha Vantage, EODHD)

Keys are **not** stored in source. Provide them in either order:

1. **Environment variables** (CI, Docker, shell): `ALPHA_VANTAGE_API_KEY`, `EODHD_API_KEY` (alias `ALPHAVANTAGE_API_KEY` is also read for Alpha).
2. **Streamlit secrets**: local file `.streamlit/secrets.toml` (gitignored) or **Streamlit Community Cloud → App settings → Secrets**.

Copy the template and edit:

```bash
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Edit secrets.toml with your keys, then:
streamlit run app.py
```

Without keys, Yahoo-only market data still runs; Alpha/EODHD fallbacks stay disabled.

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```

Optional local bind (e.g. Docker):

```bash
streamlit run app.py --server.address 0.0.0.0 --server.port 8501
```

## Deploy on Streamlit Community Cloud

1. Push this repository to GitHub (see below).
2. Open [share.streamlit.io](https://share.streamlit.io), sign in, and **New app**.
3. Pick the repo and branch (e.g. `main`).
4. Set **Main file path** to `app.py`.
5. **Advanced settings** (optional): confirm Python matches `runtime.txt` (e.g. `python-3.12`).
6. Under **App settings → Secrets**, paste TOML (same keys as in `secrets.toml.example`):

   ```toml
   ALPHA_VANTAGE_API_KEY = "your_key_here"
   EODHD_API_KEY = "your_token_here"
   ```

7. **Deploy**. Cloud runs `pip install -r requirements.txt` then `streamlit run app.py`.

Do not commit `.streamlit/secrets.toml`. If keys were ever committed to git history, rotate them in the provider dashboards before publishing the repo.

## Push to GitHub

```bash
git add -A
git status
git commit -m "Structure repo for Streamlit Cloud deployment"
git push origin main
```

If the remote is not set yet:

```bash
git remote add origin https://github.com/<user>/<repo>.git
git push -u origin main
```
