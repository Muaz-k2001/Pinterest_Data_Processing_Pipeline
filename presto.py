import prestodb
import pandas as pd

connection = prestodb.dbapi.connect(
    host='localhost',
    catalog='cassandra',
    user='muaz',
    port=8080,
    schema='data'
)

cur = connection.cursor()
cur.execute('SELECT * FROM spark_data')
rows = cur.fetchall()

spark_df = pd.DataFrame(rows)
print(spark_df)