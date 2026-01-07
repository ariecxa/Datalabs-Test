import time
import pendulum
from airflow.sdk import DAG, task
import pandas as pd
import mysql.connector

with DAG(
    dag_id="ETL_Datalabs_Test",
    schedule=None,
    catchup=False,
    tags=["ETL_Datalabs_Test"],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
) as dag:
    @task()
    def extract():
        # Extract
        print("Run Extraction Process")
        try:
            conn = mysql.connector.connect(
                host="host.docker.internal",
                user="root",
                password="",
                database="datalabs_test"
            )

            qry_extract_customers = "SELECT * FROM customers"
            pd_extract_customers = pd.read_sql(qry_extract_customers, conn)

            qry_extract_transactions = "SELECT * FROM transactions"
            pd_extract_transactions = pd.read_sql(qry_extract_transactions, conn)

            qry_extract_products = "SELECT * FROM products"
            pd_extract_products = pd.read_sql(qry_extract_products, conn)

            qry_extract_transactions_item = "SELECT * FROM transactions_items"
            pd_extract_transactions_item = pd.read_sql(qry_extract_transactions_item, conn)

            qry_extract_marketing_campaigns = "SELECT * FROM marketing_campaigns"
            pd_extract_marketing_campaigns= pd.read_sql(qry_extract_marketing_campaigns, conn)

            conn.close()
            
            pd_extract_customers.to_parquet("extract_customers.parquet")
            pd_extract_transactions.to_parquet("extract_transactions.parquet")
            pd_extract_products.to_parquet("extract_products.parquet")
            pd_extract_transactions_item.to_parquet("extract_transactions_item.parquet")
            pd_extract_marketing_campaigns.to_parquet("extract_marketing_campaigns.parquet")

            return True

        except:
            print("can't connect to MySQL")
            exit(1)

    @task()
    def transformation():
        pd_extract_customers=pd.read_parquet("extract_customers.parquet")
        pd_extract_transactions=pd.read_parquet("extract_transactions.parquet")
        pd_extract_products=pd.read_parquet("extract_products.parquet")
        pd_extract_transactions_item=pd.read_parquet("extract_transactions_item.parquet")
        pd_extract_marketing_campaigns=pd.read_parquet("extract_marketing_campaigns.parquet")

        # Transformation
        print("Run Transformation Process")
        # Membuat seluruh text menjadi huruf besar
        cols = ['name','email','city']
        pd_extract_customers[cols] = pd_extract_customers[cols].apply(lambda col: col.str.upper())

        cols = ['product_name','category']
        pd_extract_products[cols] = pd_extract_products[cols].apply(lambda col: col.str.upper())

        cols = ['campaign_name','channel']
        pd_extract_marketing_campaigns[cols] = pd_extract_marketing_campaigns[cols].apply(lambda col: col.str.upper())

        # Update Price 0 pada table transactions_item berdasarkan data products
        pd_extract_transactions_item["price"] = pd_extract_transactions_item["product_id"].map(
            pd_extract_products.set_index("product_id")["price"]
        ).astype(int)

        # Aggregasi penjumlahan total amount berdasarkan transaction id dari dari hasil price * quantity untuk mengisi table transaction
        df_transaction_total = (
            pd_extract_transactions_item
                .assign(amount=lambda df: df["price"] * df["quantity"])
                .groupby("transaction_id", as_index=False)
                .agg(total_amount=("amount", "sum"))
        )

        pd_extract_transactions = pd_extract_transactions.drop(columns="total_amount")

        pd_extract_transactions = pd_extract_transactions.merge(
            df_transaction_total,
            on="transaction_id",
            how="left"
        )

        # Table Fact Sales
        pd_fact_sales = pd_extract_transactions_item.merge(
            pd_extract_transactions,
            on="transaction_id",
            how="left"
        )

        pd_extract_customers.to_parquet("transformation_customers.parquet")
        pd_extract_products.to_parquet("transformation_products.parquet")
        pd_extract_marketing_campaigns.to_parquet("transformation_marketing_campaigns.parquet")
        pd_fact_sales.to_parquet("transformation_fact_sales.parquet")

    @task()
    def load():
        pd_extract_customers=pd.read_parquet("transformation_customers.parquet")
        pd_extract_products=pd.read_parquet("transformation_products.parquet")
        pd_extract_marketing_campaigns=pd.read_parquet("transformation_marketing_campaigns.parquet")
        pd_fact_sales=pd.read_parquet("transformation_fact_sales.parquet")

        # Load 
        print("Run Load Process")
        try:
            conn = mysql.connector.connect(
                host="host.docker.internal",
                user="root",
                password="",
                database="datalabs_test"
            )

            cursor = conn.cursor()

            # Overwrite Old Data Customer
            cursor.execute("TRUNCATE TABLE dim_customer")
            # Insert New Data Customer
            sql_dim_customers = """
            INSERT INTO dim_customer (customer_id, name, email, city, signup_date)
            VALUES (%s, %s, %s, %s, %s)
            """
            data_dim_customers = list(
                pd_extract_customers[['customer_id', 'name', 'email', 'city', 'signup_date']]
                .itertuples(index=False, name=None)
            )
            cursor.executemany(sql_dim_customers, data_dim_customers)

            # Overwrite Old Data Products
            cursor.execute("TRUNCATE TABLE dim_products")
            sql_dim_products = """
            INSERT INTO dim_products (product_id, product_name, category, price)
            VALUES (%s, %s, %s, %s)
            """

            data_dim_products = list(
                pd_extract_products[['product_id', 'product_name', 'category', 'price' ]]
                .itertuples(index=False, name=None)
            )
            cursor.executemany(sql_dim_products, data_dim_products)

            # Overwrite Old Data Campaign
            cursor.execute("TRUNCATE TABLE dim_campaign")
            sql_dim_campaign = """
            INSERT INTO dim_campaign (campaign_id, campaign_name, start_date, end_date, channel)
            VALUES (%s, %s, %s, %s, %s)
            """
            data_dim_campaign = list(
                pd_extract_marketing_campaigns[['campaign_id', 'campaign_name', 'start_date', 'end_date', 'channel']]
                .itertuples(index=False, name=None)
            )
            cursor.executemany(sql_dim_campaign, data_dim_campaign)

            # Overwrite Old Fact_Sales
            cursor.execute("TRUNCATE TABLE fact_sales")
            sql_fact_sales = """
            INSERT INTO fact_sales (transaction_item_id, product_id ,customer_id, quantity, price, total_amount, transaction_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            data_fact_sales = list(
                pd_fact_sales[['transaction_item_id', 'product_id', 'customer_id', 'quantity', 'price', 'total_amount','transaction_date']]
                .itertuples(index=False, name=None)
            )
            cursor.executemany(sql_fact_sales, data_fact_sales)

            conn.commit()
            print("ETL Process Done")
        except:
            cursor.close()
            conn.close()
            print("ETL Process Failed")
        
    extract() >> transformation() >> load()


