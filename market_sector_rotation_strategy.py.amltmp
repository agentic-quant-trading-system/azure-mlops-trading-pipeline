import os
import time
import json
import struct
from io import BytesIO
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from google import genai

# ==========================================
# --- 1. ENTERPRISE CONFIGURATION ---
# ==========================================
SERVER = 'quant-server-123.database.windows.net'  # UPDATE THIS
DATABASE = 'trading-db'                           # UPDATE THIS
DASHBOARD_URL = "https://msm-quant-dashboard.azurewebsites.net" # UPDATE THIS

# Blob Storage Config (No secrets needed, uses Managed Identity!)
ACCOUNT_URL = "https://rawtradingdata26.blob.core.windows.net"
CONTAINER_NAME = "raw-market-data"

SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "your_email@gmail.com")
RECEIVER_EMAIL = os.environ.get("RECEIVER_EMAIL", "your_email@gmail.com")
KEY_VAULT_URL = os.environ.get("KEY_VAULT_URL", "https://kv-mlworkspace26.vault.azure.net/")

print("🔐 Connecting to Azure Key Vault & Managed Identity...")
credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

# Fetch secrets dynamically at runtime
GEMINI_API_KEY = secret_client.get_secret("GEMINI-API-KEY").value
DISCORD_WEBHOOK_URL = secret_client.get_secret("DISCORD-WEBHOOK-URL").value
SENDER_PASSWORD = secret_client.get_secret("EMAIL-APP-PASSWORD").value

# Initialize Gemini Client
gemini_client = genai.Client(api_key=GEMINI_API_KEY)

# ==========================================
# --- 2. SQL CONNECTION & RETRY LOGIC ---
# ==========================================
def get_sql_engine():
    """Generates an authenticated SQLAlchemy engine using Managed Identity."""
    driver = '{ODBC Driver 17 for SQL Server}'
    token_object = credential.get_token("https://database.windows.net/.default")
    token_as_bytes = bytes(token_object.token, "UTF-8")
    encoded_token = token_as_bytes.decode("UTF-8").encode("UTF-16-LE")
    token_struct = struct.pack(f"<I{len(encoded_token)}s", len(encoded_token), encoded_token)
    
    conn_str = f"DRIVER={driver};SERVER=tcp:{SERVER},1433;DATABASE={DATABASE};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    
    def get_conn():
        return pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    
    return create_engine("mssql+pyodbc://", creator=get_conn)

def write_to_sql_with_retry(df, table_name, write_behavior='append'):
    """Writes a DataFrame to Azure SQL with sleep-retry logic."""
    max_retries = 3
    retry_delay = 15
    for attempt in range(max_retries):
        try:
            engine = get_sql_engine()
            with engine.begin() as conn:
                df.to_sql(table_name, conn, if_exists=write_behavior, index=False)
            print(f"✅ Successfully wrote to {table_name} in Azure SQL.")
            return True
        except OperationalError as e:
            print(f"Database write failed (Attempt {attempt + 1}/{max_retries}). Retrying in {retry_delay}s...")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise Exception(f"Database failed to respond: {e}")

if __name__ == "__main__":
    print("🚀 Initiating Enterprise Data & Agentic Workflow...")

    # ==========================================
    # --- PHASE 1: DATA EXTRACTION (BLOB) ---
    # ==========================================
    print("\n🔍 Scanning Blob Storage for the latest datasets...")
    blob_service_client = BlobServiceClient(account_url=ACCOUNT_URL, credential=credential)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    # Dynamically find the most recent files
    market_blobs = list(container_client.list_blobs(name_starts_with="market_data_"))
    market_blobs.sort(key=lambda x: x.last_modified, reverse=True)
    MARKET_BLOB_NAME = market_blobs[0].name

    macro_blobs = list(container_client.list_blobs(name_starts_with="macro_data_"))
    macro_blobs.sort(key=lambda x: x.last_modified, reverse=True)
    MACRO_BLOB_NAME = macro_blobs[0].name

    print(f"📥 Downloading {MARKET_BLOB_NAME}...")
    market_download = container_client.get_blob_client(MARKET_BLOB_NAME).download_blob().readall()
    df_market = pd.read_csv(BytesIO(market_download), header=[0, 1], index_col=0, parse_dates=True)

    print(f"📥 Downloading {MACRO_BLOB_NAME}...")
    macro_download = container_client.get_blob_client(MACRO_BLOB_NAME).download_blob().readall()
    macro_json = json.loads(macro_download)
    df_macro = pd.DataFrame(macro_json['observations'])

    # ==========================================
    # --- PHASE 2: DATA TRANSFORMATION & ML ---
    # ==========================================
    print("\n⚙️ Processing Data & Executing K-Means Clustering...")
    df_macro['date'] = pd.to_datetime(df_macro['date'])
    df_macro['value'] = pd.to_numeric(df_macro['value'], errors='coerce')
    df_macro = df_macro[['date', 'value']].rename(columns={'value': 'CPI'}).set_index('date')

    # Clean & Align
    if isinstance(df_market.columns, pd.MultiIndex):
        df_market.columns = [f"{col[0]}_{col[1]}" for col in df_market.columns.values]
    df_market = df_market.ffill()
    if df_market.index.tz is not None:
        df_market.index = df_market.index.tz_localize(None)

    df_macro_daily = df_macro.resample('D').ffill()
    df_merged = df_market.join(df_macro_daily, how='left')
    df_merged['CPI'] = df_merged['CPI'].ffill()

    # Feature Engineering
    df_merged['SPY_Daily_Return'] = df_merged['Close_SPY'].pct_change()
    df_merged['SPY_Volatility_20d'] = df_merged['SPY_Daily_Return'].rolling(window=20).std()
    
    features = ['SPY_Daily_Return', 'SPY_Volatility_20d', 'CPI']
    df_cleaned = df_merged.dropna(subset=features).copy()

    # K-Means Clustering
    X = df_cleaned[features]
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
    df_cleaned['Regime'] = kmeans.fit_predict(X_scaled).astype(str)
    
    # Crucial MLOps Step: Reset the index so 'Date' becomes a column before SQL upload
    df_cleaned.reset_index(inplace=True)
    df_cleaned.rename(columns={'index': 'Date'}, inplace=True)

    # ==========================================
    # --- PHASE 3: UPLOAD TO AZURE SQL ---
    # ==========================================
    print("\n💾 Uploading Processed Data to Azure SQL...")
    # We use 'replace' here because K-Means re-evaluates all historical clusters daily
    write_to_sql_with_retry(df_cleaned, 'ProcessedMarketData', write_behavior='replace')

    # ==========================================
    # --- PHASE 4: AGENTIC WORKFLOW ---
    # ==========================================
    latest_data = df_cleaned.iloc[-1]
    current_date = latest_data['Date'].strftime('%Y-%m-%d')
    current_regime = int(latest_data['Regime'])
    
    print(f"\n🧠 Requesting AI Thesis from Google Gemini for {current_date} (Regime: {current_regime})...")
    prompt = f"""
    You are an expert quantitative analyst.
    Current Market Date: {current_date}
    Market Regime: {current_regime} (0=Sideways Chop, 1=Risk-On Bull, 2=Risk-Off Shock)
    SPY Daily Return: {latest_data.get('SPY_Daily_Return', 0):.4f}
    20-Day Volatility: {latest_data.get('SPY_Volatility_20d', 0):.4f}
    Macro CPI: {latest_data.get('CPI', 0):.2f}

    Generate a JSON-formatted investment thesis with the following exact keys:
    {{
      "macro_thesis": "string",
      "sector_signals": [
        {{"ticker": "string", "name": "string", "signal": "BUY/HOLD/SELL", "rationale": "string"}}
      ],
      "risk_protocol": [
        {{"factor": "string", "signal": "string", "rationale": "string"}}
      ]
    }}
    Ensure output is ONLY valid JSON with no markdown blocks.
    """
    
    response = gemini_client.models.generate_content(
        model='gemini-2.5-flash',
        contents=prompt
    )
    daily_thesis = response.text.replace("```json", "").replace("```", "").strip()

    # Save Thesis to SQL (Append so we build a history)
    df_thesis_save = pd.DataFrame({'Date': [latest_data['Date']], 'Thesis': [daily_thesis]})
    write_to_sql_with_retry(df_thesis_save, 'AIThesis', write_behavior='append')

    # ==========================================
    # --- PHASE 5: DELIVERY SEQUENCE ---
    # ==========================================
    print("\n📬 Initiating Delivery Sequence...")

    try:
        discord_payload = {
            "content": f"🚨 **New Quant Thesis Alert - {current_date}** 🚨\n\n**Detected Market Regime:** `{current_regime}`\n\n🌐 **View live analysis:**\n{DASHBOARD_URL}"
        }
        resp = requests.post(DISCORD_WEBHOOK_URL, json=discord_payload)
        if resp.status_code == 204:
            print("✅ Successfully posted to Discord!")
    except Exception as e:
        print(f"❌ Failed to post to Discord: {e}")

    try:
        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['To'] = RECEIVER_EMAIL
        msg['Subject'] = f"Automated Trading Thesis - Regime {current_regime} ({current_date})"
        email_body = f"A new AI thesis has been generated for {current_date}.\n\nView Dashboard: {DASHBOARD_URL}\n\nRaw JSON:\n{daily_thesis}"
        msg.attach(MIMEText(email_body, 'plain'))

        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.send_message(msg)
        server.quit()
        print("✅ Successfully sent Email!")
    except Exception as e:
        print(f"❌ Failed to send Email: {e}")

    print("\n🚀 ENTERPRISE AGENTIC WORKFLOW COMPLETE.")