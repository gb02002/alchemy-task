from typing import List
from sqlalchemy import text
from others_and_setUps import users, engine


def query_users(min_age, min_avg_heart_rate, date_from, date_to) -> [List[users], float]:
    """Nested Select with wierd order, faster then others"""

    test = text('SELECT hr.user_id, avg(hr.heart_rate) FROM heart_rates hr JOIN (SELECT id FROM users WHERE age > '
                ':min_age) u ON u.id = hr.user_id WHERE timestamp BETWEEN :date_from AND :date_to GROUP BY user_id '
                'HAVING avg(heart_rate) > :min_avg_heart_rate;'
                )

    with engine.connect() as conn:
        result = conn.execute(test, {"min_age": min_age, "date_from": date_from, "date_to": date_to,
                                     "min_avg_heart_rate": min_avg_heart_rate})

    data = result.fetchall()

    return data


def query_for_user(user_id, date_from, date_to):
    """Selects 10 rows of highest avg heart_rate"""

    test = text("SELECT date_trunc('hour', timestamp) AS hour, avg(heart_rate) AS avg_heart_rate "
                "FROM heart_rates "
                "WHERE user_id = :user_id and timestamp BETWEEN :date_from AND :date_to "
                "GROUP BY hour "
                "ORDER BY avg_heart_rate DESC "
                "LIMIT 10;")

    with engine.connect() as conn:
        result = conn.execute(test, {'user_id': user_id, 'date_from': date_from, 'date_to': date_to})

    data = result.fetchall()

    return data


# Example usage
result_users_list = query_users(min_age=50, min_avg_heart_rate=85, date_to='2024-05-26 00:00:00.000000',
                                date_from='2024-05-25 00:00:00.000000')
result_ten_rows = query_for_user(user_id=15, date_to='2024-05-26 00:00:00.000000',
                                 date_from='2024-05-25 00:00:00.000000')
