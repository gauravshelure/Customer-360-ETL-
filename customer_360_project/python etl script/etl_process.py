import pandas as pd
import pymysql
import json
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get the directory of the script
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)

try:
    # Load source files with full paths
    website_path = os.path.join(parent_dir, "website_customers.csv")
    sales_path = os.path.join(parent_dir, "sales_data.csv")
    crm_path = os.path.join(parent_dir, "crm_data.json")

    logger.info("Loading source files...")
    website = pd.read_csv(website_path)
    sales = pd.read_csv(sales_path)

    with open(crm_path) as f:
        crm = pd.DataFrame(json.load(f))

    logger.info(f"Loaded {len(website)} website customers, {len(sales)} sales records, {len(crm)} CRM records")

    # Remove duplicate customers
    initial_count = len(website)
    website = website.drop_duplicates(subset="customer_id")
    logger.info(f"Removed {initial_count - len(website)} duplicate customers")

    # Convert names to uppercase
    website['name'] = website['name'].str.upper()

    # Calculate total spending
    sales_summary = sales.groupby("customer_id")["amount"].sum().reset_index()
    sales_summary.rename(columns={"amount":"total_spent"}, inplace=True)
    logger.info(f"Calculated spending for {len(sales_summary)} customers")

    # Merge all datasets
    df = website.merge(crm, on="customer_id")
    df = df.merge(sales_summary, on="customer_id")
    logger.info(f"Final dataset has {len(df)} records")

    # Connect to MySQL
    logger.info("Connecting to MySQL database...")
    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="@Gaurav08",
        database="customer360"
    )

    cursor = conn.cursor()

    # Clear existing data (optional - remove if you want to append)
    cursor.execute("TRUNCATE TABLE customer_360")
    logger.info("Cleared existing data from customer_360 table")

    # Insert into target table using batch insert for better performance
    data_to_insert = []
    for i, row in df.iterrows():
        data_to_insert.append((row.customer_id, row.name, row.email, row.city, row.membership, row.total_spent))

    cursor.executemany("""
    INSERT INTO customer_360
    VALUES (%s,%s,%s,%s,%s,%s)
    """, data_to_insert)

    conn.commit()
    logger.info(f"Successfully inserted {len(data_to_insert)} records into customer_360 table")

    print("Customer 360 ETL Completed Successfully")

except FileNotFoundError as e:
    logger.error(f"File not found: {e}")
    print("Error: Source file not found. Please check file paths.")
except pymysql.Error as e:
    logger.error(f"MySQL error: {e}")
    print("Error: Database connection or operation failed.")
except Exception as e:
    logger.error(f"Unexpected error: {e}")
    print("Error: An unexpected error occurred during ETL process.")
finally:
    if 'conn' in locals() and conn:
        conn.close()
        logger.info("Database connection closed")