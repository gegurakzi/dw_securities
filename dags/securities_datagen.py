import string
import uuid
from datetime import datetime

import numpy as np
import pandas as pd
import pymysql
from airflow.decorators import dag, task

import utils.randomizer as randomizer


def create_dag(dag_id, schedule, default_args, args):
    @dag(dag_id=dag_id, schedule=schedule, default_args=default_args, catchup=False)
    def datagen_dag():

        @task()
        def user_datagen_task(size):
            with pymysql.connect(
                    host="localhost",
                    user="malachai",
                    password="0107",
                    db="securities"
            ) as conn:
                cur = conn.cursor()
                sql = """
                INSERT INTO users (
                user_id, 
                password, 
                name, 
                status, 
                type, 
                email, 
                phone_number, 
                gender, 
                age,
                birth_date, 
                income, 
                risk_tolerance, 
                creation_timestamp, 
                update_timestamp
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                for i in range(size):
                    gender = randomizer.binary_probability(2)
                    cur.execute(sql, (
                        uuid.uuid4(),
                        uuid.uuid4().hex,
                        randomizer.name_korean(gender=gender),
                        randomizer.binary_probability(probability=20),
                        "일반고객",
                        randomizer.email(),
                        "010-0000-0000",
                        gender,
                        randomizer.age_normal(50),
                        randomizer.datetime_normal(datetime.fromordinal(710000)).strftime('%Y%m%d'),
                        randomizer.income_normal(3000000),
                        ("상", "중", "하")[np.random.randint(3)],
                        datetime.now(),
                        datetime.now()
                    ))
                conn.commit()
            return None

        @task()
        def stock_datagen_task(size):
            with pymysql.connect(
                    host="localhost",
                    user="malachai",
                    password="0107",
                    db="securities"
            ) as conn:
                cur = conn.cursor()
                sql = """
                        INSERT INTO stocks (
                            stock_ticker, 
                            exchange_code, 
                            status, 
                            name, 
                            description, 
                            sector, 
                            list_date, 
                            delist_date, 
                            creation_timestamp, 
                            update_timestamp
                        ) VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """

                for i in range(size):
                    exchange_code = randomizer.choose(["KSPI", "KDAQ", "NYSE", "NSDQ"])
                    status = randomizer.binary_probability(20)
                    delist_date = None
                    if status == 1:
                        delist_date = args["current_date"].strftime('%Y%m%d')
                    try:
                        cur.execute(sql, (
                            exchange_code + ":" + randomizer.string(4, lowercase=False),
                            exchange_code,
                            status,
                            "일반주식",
                            "",
                            np.random.choice(['0001', '0002', '0003', '0004']),
                            args["current_date"].strftime('%Y%m%d'),
                            delist_date,
                            datetime.now(),
                            datetime.now()
                        ))
                    except:
                        continue
                conn.commit()

        @task()
        def account_datagen_task(rate):
            with pymysql.connect(
                    host="localhost",
                    user="malachai",
                    password="0107",
                    db="securities"
            ) as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT u.user_id FROM users u
                    LEFT JOIN accounts a ON u.user_id = a.user_id
                    WHERE a.account_number IS NULL;
                    """)
                users_no_account = pd.DataFrame(cur.fetchall())
                sql = """
                    INSERT INTO accounts (
                    account_number,
                    user_id,
                    status,
                    type,
                    name, 
                    password, 
                    open_date, 
                    close_date, 
                    deposit, 
                    withholding, 
                    creation_timestamp, 
                    update_timestamp
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                iter = int(len(users_no_account) * rate)
                user_ids = users_no_account.sample(n=iter, random_state=6524)[0].tolist()
                for i in range(iter):
                    status_code = randomizer.binary_probability(20)
                    deposit = randomizer.income_normal(3000000) * 12
                    close_date = None
                    if status_code == 1:
                        close_date = args["current_date"].strftime('%Y%m%d')
                    try:
                        cur.execute(sql, (
                            uuid.uuid4(),
                            user_ids[i],
                            status_code,
                            "00",
                            "일반계좌",
                            uuid.uuid4().hex,
                            args["current_date"].strftime('%Y%m%d'),
                            close_date,
                            deposit,
                            deposit,
                            datetime.now(),
                            datetime.now()
                        ))
                    except:
                        continue
                conn.commit()

        @task()
        def report_datagen_task():
            with pymysql.connect(
                    host="localhost",
                    user="malachai",
                    password="0107",
                    db="securities"
            ) as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT s.stock_ticker, prevr.open FROM stocks s 
                    LEFT JOIN (SELECT r.stock_ticker,r.open FROM reports r
                        JOIN (
                            SELECT stock_ticker, MAX(creation_timestamp) AS creation_timestamp FROM reports
                            GROUP BY stock_ticker
                        ) maxr
                        ON r.stock_ticker = maxr.stock_ticker
                        AND r.creation_timestamp = maxr.creation_timestamp) prevr
                    ON s.stock_ticker = prevr.stock_ticker
                    """)
                stocks = pd.DataFrame(cur.fetchall())
                sql = """
                    INSERT INTO reports (
                    report_date, 
                    stock_ticker, 
                    previous_close, 
                    open, 
                    high, 
                    low, 
                    volume, 
                    creation_timestamp, 
                    update_timestamp
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                for i in range(len(stocks)):
                    previous_close = stocks[1][i]
                    if previous_close is not int:
                        previous_close = randomizer.price_normal(20000)
                    (low, open, high) = randomizer.low_open_high(previous_close)
                    cur.execute(sql, (
                        args["current_date"].strftime('%Y%m%d'),
                        stocks[0][i],
                        previous_close,
                        open,
                        high,
                        low,
                        randomizer.income_normal(3000000),
                        datetime.now(),
                        datetime.now()
                    ))
                conn.commit()

        @task()
        def offer_datagen_task(size):
            with pymysql.connect(
                    host="localhost",
                    user="malachai",
                    password="0107",
                    db="securities"
            ) as conn:
                cur = conn.cursor()
                cur.execute("""
                        SELECT account_number FROM accounts
                        WHERE close_date IS NULL;
                        """)
                accounts = pd.DataFrame(cur.fetchall())
                cur.execute("""
                        SELECT stock_ticker FROM stocks
                        WHERE delist_date IS NULL;
                        """)
                stocks = pd.DataFrame(cur.fetchall())
                sql = """
                        INSERT INTO offers (
                        offer_number, 
                        offer_date, 
                        account_number, 
                        stock_ticker, 
                        status, 
                        type, 
                        quantity, 
                        price, 
                        traded, 
                        not_traded, 
                        creation_timestamp, 
                        update_timestamp
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """

                for i in range(size):
                    # 00: 처리 전
                    # 01: 전체 체결
                    # 02: 일부 체결
                    # 03: 전체 미체결
                    status = np.random.choice(["00", "01", "02", "03"], p=[0, 0.9, 0.08, 0.02])
                    quantity = np.random.randint(2, 100)
                    price = np.random.randint(1000, 500000)
                    not_traded = 0
                    if status == "02":
                        not_traded = np.random.randint(quantity - 1) + 1
                    elif status == "03":
                        not_traded = quantity
                    traded = quantity - not_traded
                    cur.execute(sql, (
                        uuid.uuid4(),
                        args["current_date"].strftime('%Y%m%d'),
                        randomizer.choose(accounts[0].tolist()),
                        randomizer.choose(stocks[0].tolist()),
                        status,
                        randomizer.binary_probability(2),
                        quantity,
                        price,
                        traded,
                        not_traded,
                        datetime.now(),
                        datetime.now()
                    ))
                conn.commit()

        @task()
        def trade_datagen_task():
            with pymysql.connect(
                    host="localhost",
                    user="malachai",
                    password="0107",
                    db="securities"
            ) as conn:
                cur = conn.cursor()
                cur.execute("""
                        SELECT account_number FROM accounts
                        WHERE close_date IS NULL;
                        """)
                accounts = pd.DataFrame(cur.fetchall())
                cur.execute("""
                        SELECT offer_number, offer_date, traded, price FROM offers
                        WHERE traded != 0;
                        """)
                offers = pd.DataFrame(cur.fetchall())
                sql = """
                        INSERT INTO trades (
                        trade_number, 
                        trade_date, 
                        offer_number, 
                        seller_account_number, 
                        buyer_account_number, 
                        quantity, 
                        price, 
                        charge, 
                        creation_timestamp, 
                        update_timestamp
                        ) VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                for i in range(offers.shape[0]):
                    seller_account_number, buyer_account_number = randomizer.sample(accounts[0].tolist(), 2)
                    cur.execute(sql, (
                        uuid.uuid4(),
                        offers[1][i],
                        offers[0][i],
                        seller_account_number,
                        buyer_account_number,
                        offers[2][i],
                        offers[3][i],
                        offers[3][i] * 0.005,
                        datetime.now(),
                        datetime.now()
                    ))
                conn.commit()


        # DAG branching
        t1 = user_datagen_task(100)
        t2 = stock_datagen_task(3000)
        t3 = account_datagen_task(0.5)
        t4 = report_datagen_task()
        t5 = offer_datagen_task(200000)
        t6 = trade_datagen_task()

        [t1, t2] >> t3
        t2 >> t4
        [t2, t3] >> t5 >> t6

    return datagen_dag()


create_dag(
    "dag_securities_data_generation",
    "@daily",
    {
        "owner": "airflow",
        "start_date": datetime(2024, 1, 25),
        "hello_first": False
    },
    {
        "current_date": datetime.now()
    }
)
