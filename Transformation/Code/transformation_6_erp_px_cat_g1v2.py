import pyodbc
import pandas as pd

# ==============================================================
# TRANSFORMATION 6 — ERP Product Category (PX_CAT_G1V2.csv)
# Cleans: trim whitespace only — data is already clean
# ==============================================================

SERVER   = "LITTLE-DUY\\SQLEXPRESS"
DATABASE = "DWH"
CSV_PATH = r"C:\path\to\your\csv\files\PX_CAT_G1V2.csv"  # <-- UPDATE

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
    IF OBJECT_ID('transformation.erp_px_cat_g1v2', 'U') IS NOT NULL
        DROP TABLE transformation.erp_px_cat_g1v2;
    CREATE TABLE transformation.erp_px_cat_g1v2 (
        id          NVARCHAR(50),
        cat         NVARCHAR(100),
        subcat      NVARCHAR(100),
        maintenance NVARCHAR(10)
    );
""")
print("✅ Table transformation.erp_px_cat_g1v2 created")

# --- Transform ---
df = pd.read_csv(CSV_PATH)
df = trim_string_columns(df)
df.columns = ['id', 'cat', 'subcat', 'maintenance']

# --- Load ---
write_to_sql(df, "transformation.erp_px_cat_g1v2", cursor)

cursor.close()
conn.close()
