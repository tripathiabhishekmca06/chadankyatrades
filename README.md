# Futures Trading Dashboard

Streamlit-based trading dashboard for scanning NSE futures symbols, generating technical trade signals, and tracking active/closed trades using SQLite.

## Features

- Signal generation from EMA/RSI/ATR/volume + market regime filters
- Separate LONG and SHORT trade sections with card-based UI
- Active trades tracking with stop-loss, target, and exit-reason updates
- Closed trade history with P&L capture
- Optional sample-data mode for offline testing

## Project Structure

- `app.py` - main Streamlit app
- `requirements.txt` - Python dependencies
- `.gitignore` - ignored local/runtime files
- `README.md` - setup and deployment guide

## Run Locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

The app auto-creates `signals.db` in the project directory if it does not exist.

## Deployment (GitHub + Streamlit Cloud)

### 1) Push to GitHub

```bash
git init
git add .
git commit -m "Prepare Streamlit trading dashboard for cloud deployment"
git branch -M main
git remote add origin <repo_url>
git push -u origin main
```

### 2) Deploy on Streamlit Cloud

1. Go to [https://share.streamlit.io](https://share.streamlit.io)
2. Sign in and connect your GitHub account
3. Choose your repository and branch (`main`)
4. Set main file path to `app.py`
5. Click **Deploy**

## Notes

- `sqlite3` is part of Python standard library and is not installed via `pip`.
- Data fetching and symbol scans are cached with `@st.cache_data`.
- The app includes error handling for missing symbol files, API failures, and empty market data responses.
