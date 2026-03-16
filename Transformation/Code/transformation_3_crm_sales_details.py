import pyodbc
import pandas as pd

# ==============================================================
# TRANSFORMATION 3 — CRM Sales Details (sales_details.csv)
# Cleans: integer dates → DATE, derives missing sales/price
# ==============================================================

SERVER   = "LITTLE-DUY\\SQLEXPRESS"
DATABASE = "DWH"
CSV_PATH = r"datasets\source_crm\sales_details.csv"  # <-- UPDATE

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
    IF OBJECT_ID('transformation.crm_sales_details', 'U') IS NOT NULL
        DROP TABLE transformation.crm_sales_details;
    CREATE TABLE transformation.crm_sales_details (
        sls_ord_num  NVARCHAR(50),
        sls_prd_key  NVARCHAR(100),
        sls_cust_id  INT,
        sls_order_dt DATE,
        sls_ship_dt  DATE,
        sls_due_dt   DATE,
        sls_sales    FLOAT,
        sls_quantity INT,
        sls_price    FLOAT
    );
""")
print("✅ Table transformation.crm_sales_details created")

# --- Transform ---
df = pd.read_csv(CSV_PATH)
df = trim_string_columns(df)

# Convert integer dates YYYYMMDD → proper DATE
def int_to_date(series):
    return pd.to_datetime(series.astype(str), format='%Y%m%d', errors='coerce')

df['sls_order_dt'] = int_to_date(df['sls_order_dt'])
df['sls_ship_dt']  = int_to_date(df['sls_ship_dt'])
df['sls_due_dt']   = int_to_date(df['sls_due_dt'])

# Derive missing sales = quantity * price
df['sls_sales'] = df.apply(
    lambda r: r['sls_quantity'] * r['sls_price']
    if pd.isna(r['sls_sales']) and pd.notna(r['sls_price'])
    else r['sls_sales'], axis=1
)

# Derive missing price = sales / quantity
df['sls_price'] = df.apply(
    lambda r: r['sls_sales'] / r['sls_quantity']
    if pd.isna(r['sls_price']) and pd.notna(r['sls_sales']) and r['sls_quantity'] != 0
    else r['sls_price'], axis=1
)

# Format dates
df['sls_order_dt'] = df['sls_order_dt'].dt.strftime('%Y-%m-%d')
df['sls_ship_dt']  = df['sls_ship_dt'].dt.strftime('%Y-%m-%d')
df['sls_due_dt']   = df['sls_due_dt'].dt.strftime('%Y-%m-%d')

# --- Load ---
write_to_sql(df, "transformation.crm_sales_details", cursor)

cursor.close()
conn.close()
