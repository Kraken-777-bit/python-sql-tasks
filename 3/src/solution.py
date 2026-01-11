import psycopg2
from psycopg2.extras import execute_values

conn = psycopg2.connect('postgresql://postgres:@localhost:5432/test_db')


# BEGIN (write your solution here)
def batch_insert(connection, products):
    with connection.cursor() as cursor:
        # Преобразуем список словарей в список кортежей в правильном порядке
        data = [(p['name'], p['price'], p['quantity']) for p in products]
        
        # Используем execute_values для массовой вставки
        execute_values(
            cursor,
            "INSERT INTO products (name, price, quantity) VALUES %s",
            data
        )
    connection.commit()


def get_all_products(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM products ORDER BY price DESC")
        rows = cursor.fetchall()
        return rows
# END
