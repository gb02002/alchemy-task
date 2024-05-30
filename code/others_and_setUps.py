import random
from datetime import datetime, timedelta
from typing import List, Union, Tuple, Any, Sequence
import time
import os

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, ForeignKey, Index, Row
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

# This file contains script that will

DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 1111)
DB_USERNAME = os.getenv('POSTGRES_USERNAME', 'postgres')
DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_NAME = os.getenv('POSTGRES_TABLENAME', 'alchemy_1')
DB_PORT = os.getenv('POSTGRES_PORT', 5433)

#Create engine
engine = create_engine(f'postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}', echo=True)
# Create metadata
metadata = MetaData()

users = Table(
    'users',
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


def populate_database(session):
    """Populate db with basic data"""

    genders = ['Male', 'Female', 'Other']
    users_data = [
        {'name': f'User{i}', 'gender': random.choice(genders), 'age': random.randint(18, 65)}
        for i in range(1, 301)
    ]

    session.execute(users.insert(), users_data)
    session.commit()

    all_users = session.query(users).all()

    heart_rates_data = []
    start_time = datetime.now()

    for _ in range(10000):
        user = random.choice(all_users)
        timestamp = start_time - timedelta(minutes=random.randint(0, 60 * 24 * 30))  # последние 30 дней
        heart_rate = random.uniform(60, 100)  # средний диапазон сердцебиения

        heart_rates_data.append({
            'user_id': user.id,
            'timestamp': timestamp,
            'heart_rate': heart_rate
        })

    session.execute(heart_rates.insert(), heart_rates_data)
    session.commit()


def create_table_and_populate_it():
    """Entrypoint, sets everything around"""
    if __name__ == 'main':
        # Create tables
        metadata.create_all(engine)

        # Create session
        Session = sessionmaker(bind=engine)
        session = Session()

        populate_database(session=session)


# Other possible request functions.

def query_users_SIMPLE_JOIN(min_age, min_avg_heart_rate, date_from, date_to) -> tuple[Sequence[Row[Any]], float]:
    "Regular JOIN. Works just fine."""

    start_time = time.time()
    test = text("SELECT users.id, users.name, AVG(heart_rate) as hr_AVG "
                "from heart_rates "
                "JOIN users "
                "ON heart_rates.user_id=users.id "
                "WHERE timestamp BETWEEN :date_from AND :date_to AND users.age > :min_age "
                "GROUP BY users.id "
                "HAVING avg(heart_rate) > :min_avg_heart_rate;")

    with engine.connect() as conn:
        result = conn.execute(test, {"min_age": min_age, "date_from": date_from, "date_to": date_to,
                                     "min_avg_heart_rate": min_avg_heart_rate})

    result_time = time.time() - start_time
    data = result.fetchall()

    return data, result_time


def query_users_BUT_RIGHT_JOIN_WITH_SELECT(min_age, min_avg_heart_rate, date_from, date_to) -> tuple[Sequence[Row[Any]], float]:
    """Nested SELECT, but we JOIN heart_rates to Users."""

    start_time = time.time()
    test = text(
        'SELECT u.id, u.name, avg(hr.heart_rate) FROM users u '
        'JOIN heart_rates hr ON u.id = hr.user_id '
        'WHERE u.age > :min_age '
        'AND hr.timestamp BETWEEN :date_from AND :date_to '
        'GROUP BY u.id '
        'HAVING avg(hr.heart_rate) > :min_avg_heart_rate;'
    )

    with engine.connect() as conn:
        result = conn.execute(test, {"min_age": min_age, "date_from": date_from, "date_to": date_to,
                                     "min_avg_heart_rate": min_avg_heart_rate})

    result_time = time.time() - start_time
    data = result.fetchall()

    return data, result_time


# Entrypoint!!!
create_table_and_populate_it()

# Those scripts would invoke funcs above:

result1, time1 = query_users_SIMPLE_JOIN(min_age=50, min_avg_heart_rate=85, date_to='2024-05-26 00:00:00.000000',
                                       date_from='2024-05-25 00:00:00.000000')
result2, time2 = query_users_BUT_RIGHT_JOIN_WITH_SELECT(min_age=50, min_avg_heart_rate=85,
                                                      date_to='2024-05-26 00:00:00.000000',
                                                      date_from='2024-05-25 00:00:00.000000')

print(time1, time2)
print(result1, '\n', result2)
