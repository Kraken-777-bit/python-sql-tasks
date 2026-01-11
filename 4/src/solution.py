import psycopg2
from psycopg2.extras import DictCursor


conn = psycopg2.connect('postgresql://postgres:@localhost:5432/test_db')


# BEGIN (write your solution here)
def get_order_sum(connection, month):
    with connection.cursor(cursor_factory=DictCursor) as cursor:
        # Запрос для получения суммы заказов по покупателям за указанный месяц
        cursor.execute("""
            SELECT c.customer_name, SUM(o.total_amount) as total
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            WHERE EXTRACT(MONTH FROM o.order_date) = %s
            GROUP BY c.customer_id, c.customer_name
            ORDER BY total ASC  -- Сортируем по сумме по возрастанию
        """, (month,))
        
        results = cursor.fetchall()
        
    # Формируем строку с результатами
    output_lines = []
    for row in results:
        output_lines.append(f"Покупатель {row['customer_name']} совершил покупок на сумму {int(row['total'])}")
    
    return "\n".join(output_lines)
# END
