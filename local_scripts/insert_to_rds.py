import psycopg2
from datetime import datetime
import os
from random import choice
import time

dsn = (
    "dbname={dbname} "
    "user={user} "
    "password={password} "
    "port={port} "
    "host={host} ".format(
        dbname="orders",
        user="postgres",
        password=os.environ['password-rds'],
        port=os.environ['port-rds'],
        host=os.environ['host-rds'],
    )
)

conn = psycopg2.connect(dsn)
print("connected")
conn.set_session(autocommit=True)
cur = conn.cursor()
cur.execute(
    "create table if not exists orders("
    "created_at timestamp,"
    "order_id integer,"
    "product_name varchar(100),"
    "value float);"
)

products = {
    "casa": 500000.00,
    "carro": 69900.00,
    "moto": 7900.00,
    "caminhao": 230000.00,
    "laranja": 0.5,
    "borracha": 0.3,
    "iphone": 1000000.00,
}
idx = 0

while True:
    print(idx)
    idx += 1
    created_at = datetime.now().isoformat()
    product_name, value = choice(list(products.items()))
    cur.execute(
        f"insert into orders values ('{created_at}', {idx}, '{product_name}', {value})"
    )
    time.sleep(0.2)