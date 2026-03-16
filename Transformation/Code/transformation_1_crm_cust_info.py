import pyodbc
import pandas as pd

# ==============================================================
# TRANSFORMATION 1 — CRM Customer Info (cust_info.csv)
# Cleans: null IDs, duplicates, gender, marital status, dates
# ==============================================================

SERVER   = "LITTLE-DUY\\SQLEXPRESS"
DATABASE = "DWH"
CSV_PATH = r'datasets\source_crm\cust_info.csv'

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
    IF OBJECT_ID('transformation.crm_cust_info', 'U') IS NOT NULL
        DROP TABLE transformation.crm_cust_info;
    CREATE TABLE transformation.crm_cust_info (
        cst_id             INT,
        cst_key            NVARCHAR(50),
        cst_firstname      NVARCHAR(100),
        cst_lastname       NVARCHAR(100),
        cst_marital_status NVARCHAR(20),
        cst_gndr           NVARCHAR(20),
        cst_create_date    DATE
    );
""")
print("✅ Table transformation.crm_cust_info created")

# --- Transform ---
df = pd.read_csv(CSV_PATH)
df = trim_string_columns(df)

# Drop null IDs
df = df.dropna(subset=['cst_id'])
df['cst_id'] = df['cst_id'].astype(int)

# Remove duplicates — keep most recent record per customer
df['cst_create_date'] = pd.to_datetime(df['cst_create_date'], errors='coerce')
df = df.sort_values('cst_create_date').drop_duplicates(subset=['cst_id'], keep='last')

# Standardize gender: M → Male, F → Female, else Unknown
df['cst_gndr'] = df['cst_gndr'].map({'M': 'Male', 'F': 'Female'}).fillna('Unknown')

# Standardize marital status: M → Married, S → Single, else Unknown
df['cst_marital_status'] = df['cst_marital_status'].map(
    {'M': 'Married', 'S': 'Single'}
).fillna('Unknown')

# Format date
df['cst_create_date'] = df['cst_create_date'].dt.strftime('%Y-%m-%d')
df['cst_create_date'] = df['cst_create_date'].where(df['cst_create_date'].notna(), None)

# --- Load ---
write_to_sql(df, "transformation.crm_cust_info", cursor)

cursor.close()
conn.close()


print('datasets\source_crm\cust_info.csv')