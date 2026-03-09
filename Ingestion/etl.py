import pyodbc
import pandas as pd
import os

# ==============================================================
# CONFIGURATION
# ==============================================================
SERVER   = "LITTLE-DUY\\SQLEXPRESS"
DATABASE = "DWH"
CSV_DIR  = r"C:\path\to\your\csv\files"   # <-- UPDATE THIS PATH

conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"Trusted_Connection=yes;"
)
conn.autocommit = True
cursor = conn.cursor()
print("✅ Connected to DWH")


# ==============================================================
# STEP 2 — DDL: CREATE BRONZE SCHEMA & TABLES
# ==============================================================

# Create bronze schema (ingestion layer — raw data as-is)
cursor.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze')
""")
print("✅ Schema 'bronze' is ready")

# ----------------------------
# ERP Tables
# ----------------------------

cursor.execute("""
    IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
        DROP TABLE bronze.erp_cust_az12;
    CREATE TABLE bronze.erp_cust_az12 (
        cid     NVARCHAR(50),
        bdate   NVARCHAR(50),   -- stored as string first, cleaned later
        gen     NVARCHAR(50)
    );
""")
print("✅ Table bronze.erp_cust_az12 created")

cursor.execute("""
    IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
        DROP TABLE bronze.erp_loc_a101;
    CREATE TABLE bronze.erp_loc_a101 (
        cid     NVARCHAR(50),
        cntry   NVARCHAR(100)
    );
""")
print("✅ Table bronze.erp_loc_a101 created")

cursor.execute("""
    IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
        DROP TABLE bronze.erp_px_cat_g1v2;
    CREATE TABLE bronze.erp_px_cat_g1v2 (
        id          NVARCHAR(50),
        cat         NVARCHAR(100),
        subcat      NVARCHAR(100),
        maintenance NVARCHAR(10)
    );
""")
print("✅ Table bronze.erp_px_cat_g1v2 created")

# ----------------------------
# CRM Tables
# ----------------------------

cursor.execute("""
    IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
        DROP TABLE bronze.crm_cust_info;
    CREATE TABLE bronze.crm_cust_info (
        cst_id             INT,
        cst_key            NVARCHAR(50),
        cst_firstname      NVARCHAR(100),
        cst_lastname       NVARCHAR(100),
        cst_marital_status NVARCHAR(10),
        cst_gndr           NVARCHAR(10),
        cst_create_date    NVARCHAR(50)   -- stored as string first, cleaned later
    );
""")
print("✅ Table bronze.crm_cust_info created")

cursor.execute("""
    IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
        DROP TABLE bronze.crm_prd_info;
    CREATE TABLE bronze.crm_prd_info (
        prd_id       INT,
        prd_key      NVARCHAR(100),
        prd_nm       NVARCHAR(255),
        prd_cost     FLOAT,
        prd_line     NVARCHAR(50),
        prd_start_dt NVARCHAR(50),
        prd_end_dt   NVARCHAR(50)
    );
""")
print("✅ Table bronze.crm_prd_info created")

cursor.execute("""
    IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
        DROP TABLE bronze.crm_sales_details;
    CREATE TABLE bronze.crm_sales_details (
        sls_ord_num  NVARCHAR(50),
        sls_prd_key  NVARCHAR(100),
        sls_cust_id  INT,
        sls_order_dt INT,            -- raw integer format YYYYMMDD
        sls_ship_dt  INT,
        sls_due_dt   INT,
        sls_sales    FLOAT,
        sls_quantity INT,
        sls_price    FLOAT
    );
""")
print("✅ Table bronze.crm_sales_details created")

print("\n✅ STEP 2 COMPLETE — All bronze tables created\n")


# ==============================================================
# STEP 3 — LOAD: Insert CSV data into bronze tables
# ==============================================================

def load_csv(filepath, table, cursor, na_fill=""):
    """Load a CSV file into a SQL Server table using bulk insert via executemany."""
    df = pd.read_csv(filepath)
    df = df.where(pd.notnull(df), na_fill)   # replace NaN with empty string

    # Strip whitespace from string columns
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()

    cols   = ", ".join(df.columns)
    params = ", ".join(["?" for _ in df.columns])
    sql    = f"INSERT INTO {table} ({cols}) VALUES ({params})"

    data = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cursor.fast_executemany = True
    cursor.executemany(sql, data)
    print(f"✅ Loaded {len(data):,} rows into {table}")


# ERP
load_csv(os.path.join(CSV_DIR, "CUST_AZ12.csv"),    "bronze.erp_cust_az12",    cursor)
load_csv(os.path.join(CSV_DIR, "LOC_A101.csv"),     "bronze.erp_loc_a101",     cursor)
load_csv(os.path.join(CSV_DIR, "PX_CAT_G1V2.csv"),  "bronze.erp_px_cat_g1v2",  cursor)

# CRM
load_csv(os.path.join(CSV_DIR, "cust_info.csv"),     "bronze.crm_cust_info",    cursor)
load_csv(os.path.join(CSV_DIR, "prd_info.csv"),      "bronze.crm_prd_info",     cursor)
load_csv(os.path.join(CSV_DIR, "sales_details.csv"), "bronze.crm_sales_details",cursor)

print("\n✅ STEP 3 COMPLETE — All CSV data loaded into bronze layer")

cursor.close()
conn.close()
