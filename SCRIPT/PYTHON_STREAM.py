import requests
import time
import random
import logging
import mysql.connector
import pandas as pd

num = 0
while True:
    response = requests.get("http://127.0.0.1:8000/now")
    if response.status_code == 200:
        data = response.json()
        logging.basicConfig(
            filename="datalab_stream.log",
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(message)s"
        )

        logging.info("Waktu dari API: %s", data["datetime_wib"])
        date_only = data["datetime_wib"]

    conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="datalabs_test"
        )
    cursor = conn.cursor()
    sql = """
    INSERT INTO streaming_date (dates)
    VALUES (%s)
    """
    cursor.execute(sql, (date_only,))

    conn.commit()
    time.sleep(random.randint(1, 5))
    
    num = num+1
    if num == 15:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="datalabs_test"
        )

        qry = r"""SELECT
                    DATE_FORMAT(dates, '%Y-%m-%d %H:%i') AS menit,
                    COUNT(*) AS total_data
                FROM streaming_date
                WHERE dates >= DATE_FORMAT(dates, '%Y-%m-%d %H:%i:00')
                  AND dates <  DATE_FORMAT(dates, '%Y-%m-%d %H:%i:00') + INTERVAL 1 MINUTE
                GROUP BY menit;
                """
        read = pd.read_sql(qry, conn)
        print(read)
        num = 0
    