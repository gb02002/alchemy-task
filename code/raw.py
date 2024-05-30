from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, DateTime, ForeignKey, Index
from typing import Secuence

engine = create_engine('postgresql://username:password@host:port/database_name')
metadata = MetaData()

users = Table('users',
    metadata,
    Column('id', Integer, primary_key=True, index=True),
    Column('name', String(15)),
    Column('gender', String(30)),
    Column('age', Integer, index=True)
)

heart_rates = Table(
    'heart_rates',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey('users.id'), index=True),
    Column('timestamp', DateTime),
    Column('heart_rate', Float),
    Index('ix_heart_rates_timestamp', 'timestamp'),
    Index('ix_heart_rates_user_id_timestamp', 'user_id', 'timestamp')
)

metadata.create_all(engine)


def query_users(min_age, min_avg_heart_rate, date_from, date_to) -> Secuence[users]:
    """Nested Select with wierd order, faster then others"""
    with engine.connect() as conn:
        result = conn.execute(text('SELECT hr.user_id, avg(hr.heart_rate) FROM heart_rates hr JOIN (SELECT id FROM users WHERE age > '
                ':min_age) u ON u.id = hr.user_id WHERE timestamp BETWEEN :date_from AND :date_to GROUP BY user_id '
                'HAVING avg(heart_rate) > :min_avg_heart_rate;'), {"min_age": min_age, "date_from": date_from, "date_to": date_to,
                                     "min_avg_heart_rate": min_avg_heart_rate})
    return result.fetchall()
    
def query_for_user(user_id, date_from, date_to):
    """Selects 10 rows of highest avg heart_rate"""

    with engine.connect() as conn:
        result = conn.execute("SELECT date_trunc('hour', timestamp) AS hour, avg(heart_rate) AS avg_heart_rate "
                "FROM heart_rates "
                "WHERE user_id = :user_id and timestamp BETWEEN :date_from AND :date_to "
                "GROUP BY hour "
                "ORDER BY avg_heart_rate DESC "
                "LIMIT 10;", {'user_id': user_id, 'date_from': date_from, 'date_to': date_to})
    return result.fetchall()
