import logging
import os

import pandas as pd
import pyodbc
import requests
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from google import genai
from google.genai import types
from google.genai.errors import ServerError
from sqlalchemy import create_engine, text

# ==========================================
# --- 1. CONFIGURATION ---
# ==========================================
SERVER = 'quant-server-123.database.windows.net'
DATABASE = 'trading-db'
SQL_USER = 'CloudSA65f2d628'

DASHBOARD_URL = "https://msm-quant-dashboard.azurewebsites.net"
KEY_VAULT_URL = os.environ.get("KEY_VAULT_URL", "https://kv-ml-trading-workspace.vault.azure.net/")

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

logger.info("Connecting to Azure Key Vault...")
credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

GEMINI_API_KEY = secret_client.get_secret("GEMINI-API-KEY").value
DISCORD_WEBHOOK_URL = secret_client.get_secret("DISCORD-WEBHOOK-URL").value
SQL_PASSWORD = secret_client.get_secret("SQL-PASSWORD").value
# Initialize the Gemini client with retry logic
gemini_client = genai.Client(
    api_key=GEMINI_API_KEY,
    http_options=types.HttpOptions(
        retry_options=types.HttpRetryOptions(
            attempts=4,  # Total number of tries
            initial_delay=2.0,  # Wait 2 seconds before the first retry
            # Explicitly tell to retry on 503 (Unavailable) and 429 (Rate Limit)
            http_status_codes=[429, 503]
        )
    )
)


# ==========================================
# --- 2. SQL CONNECTION & DATA RETRIEVAL ---
# ==========================================
def get_sql_engine():
    driver = '{ODBC Driver 17 for SQL Server}'
    conn_str = f"DRIVER={driver};SERVER=tcp:{SERVER},1433;DATABASE={DATABASE};Uid={SQL_USER};Pwd={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

    def get_conn():
        return pyodbc.connect(conn_str)

    return create_engine("mssql+pyodbc://", creator=get_conn)


# ==========================================
# --- 3. THESIS GENERATION USING AI ---
# ==========================================
def generate_daily_thesis(date_str: str, latest_data: dict) -> str:
    """
    Generates a quantitative sector rotation thesis using Gemini and optionally saves it to SQL.

    Args:
        date_str (str): The date string (e.g., '2026-04-07').
        latest_data (dict): Dictionary containing 'Regime', 'SPY_Daily_Return', 'SPY_Volatility_20d', 'CPI'.

    Returns:
        str: The raw JSON string of the daily thesis.
    """

    # --- 1. CONSTRUCT THE PROMPTS ---
    system_prompt = """
You are an elite AI Portfolio Manager executing a Quantitative Sector Rotation Strategy. 
Your architecture contains two core modules:
1. DECISION ENGINE: Issues explicit BUY, SELL, or HOLD signals for specific S&P 500 sectors.
2. RISK MANAGEMENT MODULE: Dictates portfolio diversification limits and volatility adjustments.

Context on our AI Market Regimes (Derived from K-Means Clustering):
- Regime 0: "Sideways Chop" (Low volatility, flat returns, cool inflation).
- Regime 1: "Risk-On Bull Market" (Low volatility, positive returns, higher inflation ignored).
- Regime 2: "Risk-Off Shock" (High volatility, negative returns, deflationary fears).

Tracked Sectors Universe:
- XLK (Technology - Growth / Risk-On)
- XLY (Consumer Discretionary - Cyclical)
- XLF (Financials - Interest Rate Sensitive)
- XLV (Healthcare - Defensive)
- XLU (Utilities - Ultimate Safe Haven / Defensive)
"""

    user_prompt = f"""
Based on today's AI clustering data, execute the daily sector rotation protocol.

DATA INPUTS:
Date: {date_str}
Current Regime: {latest_data['Regime']}
S&P 500 Daily Return: {round(latest_data['SPY_Daily_Return'], 5)}
S&P 500 20-Day Volatility: {round(latest_data['SPY_Volatility_20d'], 5)}
Current CPI Level: {round(latest_data['CPI'], 2)}

OUTPUT FORMAT REQUIRED:
You must return ONLY a valid JSON object matching the exact structure below. Do not include markdown formatting or extra text.
{{
  "macro_thesis": "1 paragraph synthesizing the regime, volatility, and CPI.",
  "sector_signals": [
    {{"ticker": "XLK", "name": "Technology", "signal": "BUY/SELL/HOLD", "rationale": "1 sentence explanation."}},
    {{"ticker": "XLY", "name": "Consumer Discretionary", "signal": "BUY/SELL/HOLD", "rationale": "1 sentence explanation."}},
    {{"ticker": "XLF", "name": "Financials", "signal": "BUY/SELL/HOLD", "rationale": "1 sentence explanation."}},
    {{"ticker": "XLV", "name": "Healthcare", "signal": "BUY/SELL/HOLD", "rationale": "1 sentence explanation."}},
    {{"ticker": "XLU", "name": "Utilities", "signal": "BUY/SELL/HOLD", "rationale": "1 sentence explanation."}}
  ],
  "risk_protocol": [
    {{"factor": "Max Sector Allocation", "signal": "XX% CAP", "rationale": "1 sentence based on volatility."}},
    {{"factor": "Strategy Stance", "signal": "MEAN REVERSION / TREND FOLLOWING", "rationale": "1 sentence based on regime."}},
    {{"factor": "Cash Position", "signal": "XX-XX% TARGET", "rationale": "1 sentence explanation."}},
    {{"factor": "Total Equity", "signal": "XX-XX% CAP", "rationale": "1 sentence explanation."}}
  ]
}}
"""

    # --- 2. GENERATE THE THESIS WITH GEMINI SDK ---
    logger.info("Consulting the Gemini Agent...")

    try:
        daily_thesis = gemini_client.models.generate_content(
            model='gemini-3.1-pro-preview',  # High-tier model with better reasoning capabilities
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=0.4,
                response_mime_type="application/json"
            )
        )
        return daily_thesis.text

    except ServerError as e:
        logger.warning("Primary model gemini-3.1-pro-preview is unavailable: %s", e)
        logger.info("Retrying with gemini-3-flash-preview")

        daily_thesis = gemini_client.models.generate_content(
            model='gemini-3-flash-preview',  # Low-tier model with limited reasoning capabilities as fallback
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=0.4,
                response_mime_type="application/json"
            )
        )
        return daily_thesis.text


if __name__ == "__main__":
    logger.info("Initiating Agentic Workflow...")
    engine = get_sql_engine()

    # Read only the single most recent day's data
    with engine.connect() as conn:
        df_market = pd.read_sql(text("SELECT TOP 1 * FROM ProcessedMarketData ORDER BY Date DESC"), conn)

    latest_data = df_market.iloc[0]
    current_date = pd.to_datetime(latest_data['Date'])
    # Ensure the date is cleanly formatted
    date_str = current_date.strftime('%Y-%m-%d') if hasattr(current_date, 'strftime') else str(
        latest_data['Date'])

    current_regime = int(latest_data['Regime'])

    # --- THESIS GENERATION ---

    daily_thesis = generate_daily_thesis(date_str, latest_data)

    # Save to SQL
    df_thesis_save = pd.DataFrame({'Date': [date_str], 'Thesis': [daily_thesis]})
    with engine.begin() as conn:
        df_thesis_save.to_sql('AIThesis', conn, if_exists='append', index=False)
    logger.info("Saved thesis to SQL.")

    # --- NOTIFICATIONS ---
    logger.info("Sending alerts...")
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={
            "content": f"🚨 **New Quant Thesis Alert - {date_str}** 🚨\nRegime: `{current_regime}`\n🌐 **Dashboard:**\n{DASHBOARD_URL}"})
        logger.info("Discord notification sent.")
    except Exception:
        logger.exception("Discord notification failed")
