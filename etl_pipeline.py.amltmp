import os
import time
import json
from io import BytesIO
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient

# ==========================================
# --- 1. CONFIGURATION ---
# ==========================================
SERVER = 'quant-server-123.database.windows.net' 
DATABASE = 'trading-db'                           
SQL_USER = 'CloudSA65f2d628'                   

ACCOUNT_URL = "https://rawtradingdata26.blob.core.windows.net"
CONTAINER_NAME = "raw-market-data"
KEY_VAULT_URL = "https://kv-ml-trading-workspace.vault.azure.net/"

print("🔐 Authenticating & Fetching Secrets...")
credential = DefaultAzureCredential()

# Fetch SQL Password dynamically from the Vault
secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
SQL_PASSWORD = secret_client.get_secret("SQL-PASSWORD").value

# ==========================================
# --- 2. SQL CONNECTION & RETRY LOGIC ---
# ==========================================
def get_sql_engine():
    driver = '{ODBC Driver 17 for SQL Server}'
    conn_str = f"DRIVER={driver};SERVER=tcp:{SERVER},1433;DATABASE={DATABASE};Uid={SQL_USER};Pwd={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    
    def get_conn():
        return pyodbc.connect(conn_str)
    return create_engine("mssql+pyodbc://", creator=get_conn)

# ... [KEEP THE REST OF YOUR ETL CODE EXACTLY THE SAME] ...

def write_to_sql_with_retry(df, table_name, write_behavior='append'):
    max_retries = 3
    retry_delay = 15
    for attempt in range(max_retries):
        try:
            engine = get_sql_engine()
            with engine.begin() as conn:
                df.to_sql(table_name, conn, if_exists=write_behavior, index=False)
            print(f"✅ Successfully wrote to {table_name}.")
            return True
        except OperationalError as e:
            print(f"Database write failed. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)
    raise Exception("Database failed to respond after 3 attempts.")

if __name__ == "__main__":
    print("🚀 Initiating ETL Pipeline...")

    # --- EXTRACTION ---
    print("\n🔍 Scanning Blob Storage...")
    blob_service_client = BlobServiceClient(account_url=ACCOUNT_URL, credential=credential)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    market_blobs = sorted(container_client.list_blobs(name_starts_with="market_data_"), key=lambda x: x.last_modified, reverse=True)
    macro_blobs = sorted(container_client.list_blobs(name_starts_with="macro_data_"), key=lambda x: x.last_modified, reverse=True)
    
    print(f"📥 Downloading {market_blobs[0].name} and {macro_blobs[0].name}...")
    market_download = container_client.get_blob_client(market_blobs[0].name).download_blob().readall()
    df_market = pd.read_csv(BytesIO(market_download), header=[0, 1], index_col=0, parse_dates=True)

    macro_download = container_client.get_blob_client(macro_blobs[0].name).download_blob().readall()
    df_macro = pd.DataFrame(json.loads(macro_download)['observations'])

    # --- TRANSFORMATION & ML ---
    print("\n⚙️ Processing & Executing K-Means Clustering...")
    df_macro['date'] = pd.to_datetime(df_macro['date'])
    df_macro['value'] = pd.to_numeric(df_macro['value'], errors='coerce')
    df_macro = df_macro[['date', 'value']].rename(columns={'value': 'CPI'}).set_index('date')

    if isinstance(df_market.columns, pd.MultiIndex):
        df_market.columns = [f"{col[0]}_{col[1]}" for col in df_market.columns.values]
    df_market = df_market.ffill()
    if df_market.index.tz is not None:
        df_market.index = df_market.index.tz_localize(None)

    df_merged = df_market.join(df_macro.resample('D').ffill(), how='left')
    df_merged['CPI'] = df_merged['CPI'].ffill()

    df_merged['SPY_Daily_Return'] = df_merged['Close_SPY'].pct_change()
    df_merged['SPY_Volatility_20d'] = df_merged['SPY_Daily_Return'].rolling(window=20).std()
    
    features = ['SPY_Daily_Return', 'SPY_Volatility_20d', 'CPI']
    df_cleaned = df_merged.dropna(subset=features).copy()

    X_scaled = StandardScaler().fit_transform(df_cleaned[features])
    df_cleaned['Regime'] = KMeans(n_clusters=3, random_state=42, n_init=10).fit_predict(X_scaled).astype(str)
    
    df_cleaned.reset_index(inplace=True)
    df_cleaned.rename(columns={'index': 'Date'}, inplace=True)

    # --- LOAD ---
    print("\n💾 Uploading Processed Data to Azure SQL...")
    write_to_sql_with_retry(df_cleaned, 'ProcessedMarketData', write_behavior='replace')
    print("\n✅ ETL PIPELINE COMPLETE.")