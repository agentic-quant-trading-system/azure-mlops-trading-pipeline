import os
import time
import struct
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from google import genai

# ==========================================
# --- 1. CONFIGURATION ---
# ==========================================
SERVER = 'quant-server-123.database.windows.net'
DATABASE = 'trading-db'                         
SQL_USER = 'CloudSA65f2d628'   

DASHBOARD_URL = "https://msm-quant-dashboard.azurewebsites.net"
KEY_VAULT_URL = os.environ.get("KEY_VAULT_URL", "https://kv-ml-trading-workspace.vault.azure.net/")

print("🔐 Connecting to Azure Key Vault...")
credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

GEMINI_API_KEY = secret_client.get_secret("GEMINI-API-KEY").value
DISCORD_WEBHOOK_URL = secret_client.get_secret("DISCORD-WEBHOOK-URL").value
SQL_PASSWORD = secret_client.get_secret("SQL-PASSWORD").value
gemini_client = genai.Client(api_key=GEMINI_API_KEY)

# ==========================================
# --- 2. SQL CONNECTION & DATA RETRIEVAL ---
# ==========================================
def get_sql_engine():
    driver = '{ODBC Driver 17 for SQL Server}'
    conn_str = f"DRIVER={driver};SERVER=tcp:{SERVER},1433;DATABASE={DATABASE};Uid={SQL_USER};Pwd={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    
    def get_conn():
        return pyodbc.connect(conn_str)
    return create_engine("mssql+pyodbc://", creator=get_conn)

if __name__ == "__main__":
    print("🚀 Initiating Agentic Workflow...")
    engine = get_sql_engine()
    
    # Read only the single most recent day's data
    with engine.connect() as conn:
        df_market = pd.read_sql(text("SELECT TOP 1 * FROM ProcessedMarketData ORDER BY Date DESC"), conn)
    
    latest_data = df_market.iloc[0]
    current_date = pd.to_datetime(latest_data['Date']).strftime('%Y-%m-%d')
    current_regime = int(latest_data['Regime'])

    # --- GENERATION ---
    print(f"\n🧠 Requesting AI Thesis for {current_date} (Regime: {current_regime})...")
    prompt = f"""
    You are an expert quantitative analyst.
    Market Date: {current_date} | Regime: {current_regime}
    SPY Return: {latest_data.get('SPY_Daily_Return', 0):.4f} | Volatility: {latest_data.get('SPY_Volatility_20d', 0):.4f} | CPI: {latest_data.get('CPI', 0):.2f}
    
    Generate a JSON-formatted investment thesis with keys: "macro_thesis", "sector_signals", "risk_protocol". ONLY output JSON.
    """
    response = gemini_client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
    daily_thesis = response.text.replace("```json", "").replace("```", "").strip()

    # Save to SQL
    df_thesis_save = pd.DataFrame({'Date': [latest_data['Date']], 'Thesis': [daily_thesis]})
    with engine.begin() as conn:
        df_thesis_save.to_sql('AIThesis', conn, if_exists='append', index=False)
    print("✅ Saved Thesis to SQL.")

    # --- NOTIFICATIONS ---
    print("\n📬 Sending Alerts...")
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": f"🚨 **New Quant Thesis Alert - {current_date}** 🚨\nRegime: `{current_regime}`\n🌐 **Dashboard:**\n{DASHBOARD_URL}"})
        print("✅ Discord sent!")
    except Exception as e: print(f"❌ Discord failed: {e}")