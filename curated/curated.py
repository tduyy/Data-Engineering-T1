import pyodbc
import pandas as pd

# ==============================================================
# Curated Layer
# ==============================================================

# 1) Connect to the database
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

# Load source table into dataframes
customer_crm_df = pd.read_sql("SELECT * FROM transformation.crm_cust_info",conn)
customer_erp_df = pd.read_sql("SELECT * FROM transformation.erp_cust_az12",conn)
location_erp_df = pd.read_sql("SELECT * FROM transformation.erp_loc_a101",conn)

print("✅ Data loaded")

# Joins
df = pd.merge(
    left = customer_crm_df,
    right = customer_erp_df,
    how = "left",
    left_on = "cst_key",
    right_on= "cid",
    suffixes=("","_loc")
)

    # location
df = pd.merge(
    left = df,
    right = location_erp_df,
    how = "left",
    left_on = "cst_key",
    right_on= "cid",
    suffixes=("","_loc")
)

print("✅ Joins done")

dim_customer = pd.DataFrame({
    "customer_id": df["cst_id"],
    "customer_number": df["cst_key"],
    "first_name" : df["cst_firstname"],
    "last_name" : df["cst_lastname"],
    "country" : df["cntry"],
    "marital_status": df["cst_marital_status"],
    "gender" : df["cst_gndr"],
    "birthdate" : df["bdate"],
    "create_date": df["cst_create_date"]
})

dim_customer = dim_customer.sort_values("customer_id").reset_index(drop=True)

# Create a surrogate key with auto-incrementation
dim_customer.insert(0,"customer_key",dim_customer.index + 1)

print("✅ Dimension Created")

################################################
# 2) Product Dimension
################################################

# Load source table into dataframes
product_crm_df = pd.read_sql("SELECT * FROM transformation.crm_prd_info",conn)
product_erp_df = pd.read_sql("SELECT * FROM transformation.erp_px_cat_g1v2",conn)

print("✅ Data loaded")

# Joins
df = pd.merge(
    left= product_crm_df,
    right = product_erp_df,
    how = "left",
    left_on = "prd_key",
    right_on = "prd_id",
    suffixes = ("","_cat"),
)

print("✅ Joins done")
# Creating the dimension
dim_products = pd.DataFrame({
    # "product_id": df["prd_id"],
    "product_number" : df["prd_key"],
    "product_name": df["prd_nm"],
    "category_id" : df["cat_id"],
    "category": df["cat"],
    "subcategory":df["subcat"],
    "maintenance": df["maintenance"],
    "cost": df["prd_cost"],
    "prouct_line":df["prd_line"],
    "start_date": df["prd_start_dt"],
    "end_date": df["prd_end_dt"]
})


dim_products = dim_products.sort_values("product_number").reset_index(drop=True)

# Create a surrogate key with auto-incrementation
dim_products.insert(0,"product_number",dim_products.index + 1)

print("✅ Dimension Created")

# Join Sales to Product
df =pd.merge(
    left= sales_detail_df,
    right= dim_products_df[{product_key,product_id}]  # We only want to extract these two to avoid removing the resst later

)

# Creating the fact table
fact_sales = pd.DataFrame({

})