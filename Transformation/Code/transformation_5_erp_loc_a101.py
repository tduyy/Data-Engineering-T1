import pyodbc
import pandas as pd

# ==============================================================
# TRANSFORMATION 5 — ERP Location (LOC_A101.csv)
# Cleans: removes dashes from CID, normalizes country names
# ==============================================================

SERVER   = "LITTLE-DUY\\SQLEXPRESS"
DATABASE = "DWH"
CSV_PATH = r"C:\path\to\your\csv\files\LOC_A101.csv"  # <-- UPDATE

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
    IF OBJECT_ID('transformation.erp_loc_a101', 'U') IS NOT NULL
        DROP TABLE transformation.erp_loc_a101;
    CREATE TABLE transformation.erp_loc_a101 (
        cid   NVARCHAR(50),
        cntry NVARCHAR(100)
    );
""")
print("✅ Table transformation.erp_loc_a101 created")

# --- Transform ---
df = pd.read_csv(CSV_PATH)
df = trim_string_columns(df)

# Remove dashes from CID (e.g. 'AW-00011000' → 'AW00011000')
df['CID'] = df['CID'].str.replace('-', '', regex=False)

# Normalize country names
country_map = {
    'USA': 'United States',
    'US':  'United States',
    'DE':  'Germany',
}
df['CNTRY'] = df['CNTRY'].replace(country_map).fillna('Unknown')

df.columns = ['cid', 'cntry']

# --- Load ---
write_to_sql(df, "transformation.erp_loc_a101", cursor)

cursor.close()
conn.close()
