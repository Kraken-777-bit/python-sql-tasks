import psycopg2
from psycopg2.extras import DictCursor


conn = psycopg2.connect('postgresql://postgres:@localhost:5432/test_db')


# BEGIN (write your solution here)
def create_post(connection, post_data):
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO posts (title, content, author_id) VALUES (%s, %s, %s) RETURNING id",
            (post_data['title'], post_data['content'], post_data['author_id'])
        )
        post_id = cursor.fetchone()[0]
        connection.commit()
        return post_id


def add_comment(connection, comment_data):
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO comments (post_id, author_id, content) VALUES (%s, %s, %s) RETURNING id",
            (comment_data['post_id'], comment_data['author_id'], comment_data['content'])
        )
        comment_id = cursor.fetchone()[0]
        connection.commit()
        return comment_id


def get_latest_posts(connection, n):
    with connection.cursor(cursor_factory=DictCursor) as cursor:
        # Получаем последние n постов
        cursor.execute("""
            SELECT id, title, content, author_id, created_at
            FROM posts 
            ORDER BY created_at DESC 
            LIMIT %s
        """, (n,))
        
        posts = cursor.fetchall()
        
        result = []
        for post in posts:
            # Для каждого поста получаем его комментарии
            cursor.execute("""
                SELECT id, author_id, content, created_at
                FROM comments 
                WHERE post_id = %s
                ORDER BY created_at
            """, (post['id'],))
            
            comments = cursor.fetchall()
            
            # Преобразуем пост в словарь с комментариями
            post_dict = {
                'id': post['id'],
                'title': post['title'],
                'content': post['content'],
                'author_id': post['author_id'],
                'created_at': post['created_at'],
                'comments': [dict(comment) for comment in comments]
            }
            
            result.append(post_dict)
        
        return result
# END
