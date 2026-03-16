import pyodbc
import pandas as pd

# ==============================================================
# TRANSFORMATION 4 — ERP Customer Demographics (CUST_AZ12.csv)
# Cleans: removes 'NAS' prefix from CID, standardizes gender
# ==============================================================

SERVER   = "LITTLE-DUY\\SQLEXPRESS"
DATABASE = "DWH"
CSV_PATH = r"datasets\source_erp\CUST_AZ12.csv"  # <-- UPDATE

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
    IF OBJECT_ID('transformation.erp_cust_az12', 'U') IS NOT NULL
        DROP TABLE transformation.erp_cust_az12;
    CREATE TABLE transformation.erp_cust_az12 (
        cid   NVARCHAR(50),
        bdate DATE,
        gen   NVARCHAR(20)
    );
""")
print("✅ Table transformation.erp_cust_az12 created")

# --- Transform ---
df = pd.read_csv(CSV_PATH)
df = trim_string_columns(df)

# Remove 'NAS' prefix from CID (e.g. 'NASAW00011000' → 'AW00011000')
df['CID'] = df['CID'].str.replace(r'^NAS', '', regex=True)

# Standardize gender — handles 'Male'/'Female', 'M'/'F', spaces, nulls
def standardize_gender(val):
    if pd.isna(val) or str(val).strip() == '':
        return 'Unknown'
    val = str(val).strip().upper()
    if val in ['M', 'MALE']:
        return 'Male'
    elif val in ['F', 'FEMALE']:
        return 'Female'
    return 'Unknown'

df['GEN']   = df['GEN'].apply(standardize_gender)
df['BDATE'] = pd.to_datetime(df['BDATE'], errors='coerce').dt.strftime('%Y-%m-%d')
df['BDATE'] = df['BDATE'].where(df['BDATE'].notna(), None)

df.columns = ['cid', 'bdate', 'gen']

# --- Load ---
write_to_sql(df, "transformation.erp_cust_az12", cursor)

cursor.close()
conn.close()
