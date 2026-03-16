import pyodbc
import pandas as pd

# ==============================================================
# TRANSFORMATION 2 — CRM Product Info (prd_info.csv)
# Cleans: duplicates, null costs, product line codes, dates
# Derives: cat_id from product key
# ==============================================================

SERVER   = "LITTLE-DUY\\SQLEXPRESS"
DATABASE = "DWH"
CSV_PATH = r"datasets\source_crm\prd_info.csv"

conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"Trusted_Connection=yes;"
)
conn.autocommit = True
cursor = conn.cursor()
print("✅ Connected to DWH")

# --- Helpers ---
def trim_string_columns(df):
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()
    return df

def write_to_sql(df, table, cursor):
    cols   = ", ".join(df.columns)
    params = ", ".join(["?" for _ in df.columns])
    sql    = f"INSERT INTO {table} ({cols}) VALUES ({params})"
    data   = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cursor.fast_executemany = True
    cursor.executemany(sql, data)
    print(f"✅ Loaded {len(data):,} rows into {table}")

# --- DDL ---
cursor.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'transformation')
    EXEC('CREATE SCHEMA transformation')
""")

cursor.execute("""
    IF OBJECT_ID('transformation.crm_prd_info', 'U') IS NOT NULL
        DROP TABLE transformation.crm_prd_info;
    CREATE TABLE transformation.crm_prd_info (
        prd_id       INT,
        cat_id       NVARCHAR(50),
        prd_key      NVARCHAR(100),
        prd_nm       NVARCHAR(255),
        prd_cost     FLOAT,
        prd_line     NVARCHAR(50),
        prd_start_dt DATE,
        prd_end_dt   DATE
    );
""")
print("✅ Table transformation.crm_prd_info created")

# --- Transform ---
df = pd.read_csv(CSV_PATH)
df = trim_string_columns(df)

# Extract category ID from product key (e.g. 'CO-RF-FR-R92B-58' → 'RF_FR')
df['cat_id'] = df['prd_key'].str.split('-').str[1:3].str.join('_').str.upper()

# Remove 'CO-' prefix to align key with sales transaction format
df['prd_key'] = df['prd_key'].str.replace(r'^CO-', '', regex=True)

# Deduplicate — keep latest record per product key
df['prd_start_dt'] = pd.to_datetime(df['prd_start_dt'], errors='coerce')
df['prd_end_dt']   = pd.to_datetime(df['prd_end_dt'],   errors='coerce')
df = df.sort_values(['prd_key', 'prd_start_dt'])
df = df.drop_duplicates(subset=['prd_key'], keep='last')

# Fill missing cost with 0
df['prd_cost'] = df['prd_cost'].fillna(0)

# Standardize product line codes
prd_line_map = {'R': 'Road', 'M': 'Mountain', 'S': 'Other Sales', 'T': 'Touring'}
df['prd_line'] = df['prd_line'].map(prd_line_map).fillna('Unknown')

# Format dates
df['prd_start_dt'] = df['prd_start_dt'].dt.strftime('%Y-%m-%d')
df['prd_end_dt']   = df['prd_end_dt'].dt.strftime('%Y-%m-%d')
df['prd_start_dt'] = df['prd_start_dt'].where(df['prd_start_dt'].notna(), None)
df['prd_end_dt']   = df['prd_end_dt'].where(df['prd_end_dt'].notna(), None)

# Reorder columns to match table
df = df[['prd_id', 'cat_id', 'prd_key', 'prd_nm', 'prd_cost', 'prd_line', 'prd_start_dt', 'prd_end_dt']]

# --- Load ---
write_to_sql(df, "transformation.crm_prd_info", cursor)

cursor.close()
conn.close()
