# Futures Trading Dashboard

Streamlit app for scanning an NSE-style FNO symbol list, generating technical trade signals, and tracking active and closed trades in SQLite.

## Repository layout

| Path | Purpose |
|------|---------|
| `app.py` | Main Streamlit entrypoint |
| `data/fno_list.csv` | Universe CSV (`symbol` column); required for scans |
| `data/options_list.csv` | Optional options universe (omit file if unused) |
| `requirements.txt` | Python dependencies for Cloud and local installs |
| `runtime.txt` | Python version for [Streamlit Community Cloud](https://share.streamlit.io) |
| `.streamlit/config.toml` | Portable Streamlit settings (no host or port overrides) |
| `.gitignore` | Local venv, DB, logs, cache, secrets |

Runtime SQLite (`signals.db`) and logs are created next to `app.py` on first run and are not committed.

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
3. Pick the repo and branch (`main`).
4. Set **Main file path** to `app.py`.
5. **Deploy**. Cloud installs from `requirements.txt` and uses `runtime.txt` for Python.

Secrets are optional. If you add API keys later, use **App settings → Secrets** and do not commit `.streamlit/secrets.toml`.

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
