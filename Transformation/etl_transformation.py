import pyodbc
import pandas as pd

# ==============================================================
# CONFIGURATION
# ==============================================================
SERVER   = "LITTLE-DUY\\SQLEXPRESS"
DATABASE = "DWH"

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
# HELPER FUNCTIONS
# ==============================================================

def trim_string_columns(df):
    """Strip leading/trailing whitespace from all string columns."""
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()
    return df

def write_to_sql(df, table, cursor):
    """Bulk insert a DataFrame into a SQL Server table."""
    cols   = ", ".join(df.columns)
    params = ", ".join(["?" for _ in df.columns])
    sql    = f"INSERT INTO {table} ({cols}) VALUES ({params})"
    data   = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cursor.fast_executemany = True
    cursor.executemany(sql, data)
    print(f"  ✅ Loaded {len(data):,} rows into {table}")


# ==============================================================
# STEP 1 — CREATE SILVER SCHEMA & TABLES (DDL)
# ==============================================================

cursor.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'transformation')
    EXEC('CREATE SCHEMA transformation')
""")
print("✅ Schema 'transformation' is ready")

# --- transformation.crm_cust_info ---
cursor.execute("""
    IF OBJECT_ID('transformation.crm_cust_info', 'U') IS NOT NULL DROP TABLE transformation.crm_cust_info;
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

# --- transformation.crm_prd_info ---
cursor.execute("""
    IF OBJECT_ID('transformation.crm_prd_info', 'U') IS NOT NULL DROP TABLE transformation.crm_prd_info;
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

# --- transformation.crm_sales_details ---
cursor.execute("""
    IF OBJECT_ID('transformation.crm_sales_details', 'U') IS NOT NULL DROP TABLE transformation.crm_sales_details;
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

# --- transformation.erp_cust_az12 ---
cursor.execute("""
    IF OBJECT_ID('transformation.erp_cust_az12', 'U') IS NOT NULL DROP TABLE transformation.erp_cust_az12;
    CREATE TABLE transformation.erp_cust_az12 (
        cid   NVARCHAR(50),
        bdate DATE,
        gen   NVARCHAR(20)
    );
""")

# --- transformation.erp_loc_a101 ---
cursor.execute("""
    IF OBJECT_ID('transformation.erp_loc_a101', 'U') IS NOT NULL DROP TABLE transformation.erp_loc_a101;
    CREATE TABLE transformation.erp_loc_a101 (
        cid   NVARCHAR(50),
        cntry NVARCHAR(100)
    );
""")

# --- transformation.erp_px_cat_g1v2 ---
cursor.execute("""
    IF OBJECT_ID('transformation.erp_px_cat_g1v2', 'U') IS NOT NULL DROP TABLE transformation.erp_px_cat_g1v2;
    CREATE TABLE transformation.erp_px_cat_g1v2 (
        id          NVARCHAR(50),
        cat         NVARCHAR(100),
        subcat      NVARCHAR(100),
        maintenance NVARCHAR(10)
    );
""")

print("✅ All transformation tables created\n")


# ==============================================================
# STEP 2 — TRANSFORM & LOAD EACH TABLE
# ==============================================================

# --------------------------------------------------------------
# 2a. CRM — Customer Info
# Issues found:
#   - 3 null cst_id  → drop
#   - 8 duplicate cst_id → keep latest (by cst_create_date)
#   - Whitespace in name columns → trim
#   - Gender: 'M'/'F'/null — standardize to 'Male'/'Female'/'Unknown'
#   - Marital status: 'M'/'S'/null — standardize to 'Married'/'Single'/'Unknown'
# --------------------------------------------------------------
print("🔄 Transforming transformation.crm_cust_info ...")

df = pd.read_csv('/mnt/user-data/uploads/cust_info.csv')
df = trim_string_columns(df)

# Drop rows with null ID
df = df.dropna(subset=['cst_id'])
df['cst_id'] = df['cst_id'].astype(int)

# Keep most recent record per customer (deduplicate)
df['cst_create_date'] = pd.to_datetime(df['cst_create_date'], errors='coerce')
df = df.sort_values('cst_create_date').drop_duplicates(subset=['cst_id'], keep='last')

# Standardize gender
df['cst_gndr'] = df['cst_gndr'].map({'M': 'Male', 'F': 'Female'}).fillna('Unknown')

# Standardize marital status
df['cst_marital_status'] = df['cst_marital_status'].map(
    {'M': 'Married', 'S': 'Single'}
).fillna('Unknown')

# Convert date to string for SQL insertion (DATE compatible)
df['cst_create_date'] = df['cst_create_date'].dt.strftime('%Y-%m-%d')
df['cst_create_date'] = df['cst_create_date'].where(df['cst_create_date'].notna(), None)

write_to_sql(df, "transformation.crm_cust_info", cursor)


# --------------------------------------------------------------
# 2b. CRM — Product Info
# Issues found:
#   - 102 duplicate prd_key → keep current record (where prd_end_dt is null)
#   - prd_line has trailing spaces → trim + map to full names
#   - prd_cost: 2 nulls → fill with 0
#   - prd_key contains category prefix (e.g. 'CO-RF-FR-R92B-58')
#     → extract cat_id from positions 0-5 to join with erp_px_cat_g1v2
#   - prd_key: remove leading 'CO-' prefix to match sales sls_prd_key format
# --------------------------------------------------------------
print("🔄 Transforming transformation.crm_prd_info ...")

df = pd.read_csv('/mnt/user-data/uploads/prd_info.csv')
df = trim_string_columns(df)

# Extract category ID from product key (chars 3-7, e.g. 'CO-RF-...' → 'RF')
# The key format is XX-CC-... where CC maps to category
df['cat_id'] = df['prd_key'].str.split('-').str[1:3].str.join('_').str.upper()

# Remove 'CO-' prefix from prd_key to align with sales transaction keys
df['prd_key'] = df['prd_key'].str.replace(r'^CO-', '', regex=True)

# Keep only current/active products (prd_end_dt is null = still active)
# For duplicates, keep the one without an end date, else keep latest start date
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

# Reorder columns to match table schema
df = df[['prd_id', 'cat_id', 'prd_key', 'prd_nm', 'prd_cost', 'prd_line', 'prd_start_dt', 'prd_end_dt']]

write_to_sql(df, "transformation.crm_prd_info", cursor)


# --------------------------------------------------------------
# 2c. CRM — Sales Details
# Issues found:
#   - Dates stored as integers YYYYMMDD → convert to DATE
#   - sls_sales / sls_price: 8/7 nulls → derive from each other
#     Rule: sls_sales = sls_quantity * sls_price (if one is missing)
# --------------------------------------------------------------
print("🔄 Transforming transformation.crm_sales_details ...")

df = pd.read_csv('/mnt/user-data/uploads/sales_details.csv')
df = trim_string_columns(df)

# Convert integer dates YYYYMMDD → proper dates
def int_to_date(series):
    return pd.to_datetime(series.astype(str), format='%Y%m%d', errors='coerce')

df['sls_order_dt'] = int_to_date(df['sls_order_dt'])
df['sls_ship_dt']  = int_to_date(df['sls_ship_dt'])
df['sls_due_dt']   = int_to_date(df['sls_due_dt'])

# Fix missing sales/price using: sales = quantity * price
df['sls_sales'] = df.apply(
    lambda r: r['sls_quantity'] * r['sls_price']
    if pd.isna(r['sls_sales']) and pd.notna(r['sls_price'])
    else r['sls_sales'], axis=1
)
df['sls_price'] = df.apply(
    lambda r: r['sls_sales'] / r['sls_quantity']
    if pd.isna(r['sls_price']) and pd.notna(r['sls_sales']) and r['sls_quantity'] != 0
    else r['sls_price'], axis=1
)

# Format dates
df['sls_order_dt'] = df['sls_order_dt'].dt.strftime('%Y-%m-%d')
df['sls_ship_dt']  = df['sls_ship_dt'].dt.strftime('%Y-%m-%d')
df['sls_due_dt']   = df['sls_due_dt'].dt.strftime('%Y-%m-%d')

write_to_sql(df, "transformation.crm_sales_details", cursor)


# --------------------------------------------------------------
# 2d. ERP — Customer Demographics (CUST_AZ12)
# Issues found:
#   - CID has 'NAS' prefix (e.g. 'NASAW00011000') → strip to match CRM key
#   - GEN has mixed formats: 'Male'/'Female', 'M'/'F', spaces → standardize
# --------------------------------------------------------------
print("🔄 Transforming transformation.erp_cust_az12 ...")

df = pd.read_csv('/mnt/user-data/uploads/CUST_AZ12.csv')
df = trim_string_columns(df)

# Strip 'NAS' prefix from CID to align with CRM customer key format
df['CID'] = df['CID'].str.replace(r'^NAS', '', regex=True)

# Standardize gender
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
write_to_sql(df, "transformation.erp_cust_az12", cursor)


# --------------------------------------------------------------
# 2e. ERP — Location (LOC_A101)
# Issues found:
#   - CID format 'AW-00011000' → strip dashes to match CRM key 'AW00011000'
#   - Country has duplicates: 'USA'/'United States', 'DE'/'Germany' → normalize
#   - 332 null countries → fill with 'Unknown'
# --------------------------------------------------------------
print("🔄 Transforming transformation.erp_loc_a101 ...")

df = pd.read_csv('/mnt/user-data/uploads/LOC_A101.csv')
df = trim_string_columns(df)

# Remove dashes from CID to align with CRM key format
df['CID'] = df['CID'].str.replace('-', '', regex=False)

# Normalize country names
country_map = {
    'USA': 'United States',
    'US':  'United States',
    'DE':  'Germany',
}
df['CNTRY'] = df['CNTRY'].replace(country_map).fillna('Unknown')

df.columns = ['cid', 'cntry']
write_to_sql(df, "transformation.erp_loc_a101", cursor)


# --------------------------------------------------------------
# 2f. ERP — Product Category (PX_CAT_G1V2)
# No major issues — just trim and load
# --------------------------------------------------------------
print("🔄 Transforming transformation.erp_px_cat_g1v2 ...")

df = pd.read_csv('/mnt/user-data/uploads/PX_CAT_G1V2.csv')
df = trim_string_columns(df)
df.columns = ['id', 'cat', 'subcat', 'maintenance']
write_to_sql(df, "transformation.erp_px_cat_g1v2", cursor)


# ==============================================================
print("\n✅ SILVER LAYER COMPLETE — All tables cleaned & loaded")
# ==============================================================

cursor.close()
conn.close()
