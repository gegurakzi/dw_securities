from datetime import datetime

from airflow.decorators import dag, task

from utils.delivery import extract_mysql_to_hdfs, query_hive_current_sequence, command_hive
from dimensions import users
from facts import registers

@dag(dag_id="dag_securities_etl",
     schedule="@daily",
     default_args={
            "owner": "airflow",
            "start_date": datetime(2024, 1, 25),
            "hello_first": False
        },
     catchup=False
     )
def etl_dag():

    @task()
    def dim_users_extract(**context):
        users.extract(context)

    @task()
    def dim_users_transform(**context):
        users.transform(context)

    @task()
    def dim_users_load(**context):
        users.load(context)

    @task()
    def dim_accounts_extract(**context):
        logical_date = context['ds_nodash']
        if 'logical_date' in context['params'] and context['params']['logical_date'] is not None:
            logical_date = context['params']['logical_date']
        conn_args = {
            'mysql_host': "localhost",
            'mysql_user': "",
            'mysql_pass': "",
            'mysql_db': "securities",
            'hdfs_host': "",
            'hdfs_port': "9870"
        }
        hdfs_dir_insert = "/user/airflow/etl/securities/" + logical_date + "/dim_accounts_inserted"
        hdfs_dir_update = "/user/airflow/etl/securities/" + logical_date + "/dim_accounts_updated"

        # Extract
        extract_mysql_to_hdfs(
            **conn_args,
            query="""
                        SELECT account_number, status, type, name, open_date, close_date, deposit, withholding, creation_timestamp, update_timestamp FROM accounts
                        WHERE creation_timestamp >= DATE_ADD(STR_TO_DATE('{}', '%Y%m%d'), INTERVAL -1 DAY);
                        """.format(logical_date),
            hdfs_dir=hdfs_dir_insert,
            hdfs_file=datetime.now().strftime("%Y%m%dT%H%M%S") + ".csv"
        )
        extract_mysql_to_hdfs(
            **conn_args,
            query="""
                        SELECT account_number, status, type, name, open_date, close_date, deposit, withholding, creation_timestamp, update_timestamp FROM accounts
                        WHERE creation_timestamp < DATE_ADD(STR_TO_DATE('{}', '%Y%m%d'), INTERVAL -1 DAY)
                            AND update_timestamp >= DATE_ADD(STR_TO_DATE('{}', '%Y%m%d'), INTERVAL -1 DAY);
                        """.format(logical_date, logical_date),
            hdfs_dir=hdfs_dir_update,
            hdfs_file=datetime.now().strftime("%Y%m%dT%H%M%S") + ".csv"
        )

    @task()
    def dim_accounts_transform(**context):
        logical_date = context['ds_nodash']
        if 'logical_date' in context['params'] and context['params']['logical_date'] is not None:
            logical_date = context['params']['logical_date']
        hdfs_dir_insert = "/user/airflow/etl/securities/" + logical_date + "/dim_accounts_inserted"
        hdfs_dir_update = "/user/airflow/etl/securities/" + logical_date + "/dim_accounts_updated"

        # insert.csv to external table
        hive_conn = {
            'host': "server.malachai.io",
            'port': 10000,
            'username': "",
            'password': "",
            'database': "securities",
        }
        command_hive(
            **hive_conn,
            hql="""
                    ALTER TABLE securities.TEMP_DIM_ACCOUNTS_INSERTED
                    SET LOCATION 'hdfs://{}'
                    """.format(hdfs_dir_insert)
        )
        # update.csv to external table
        command_hive(
            **hive_conn,
            hql="""
                    ALTER TABLE securities.TEMP_DIM_ACCOUNTS_UPDATED
                    SET LOCATION 'hdfs://{}'
                    """.format(hdfs_dir_update)
        )

    @task()
    def dim_accounts_load():
        # inserted
        hive_conn = {
            'host': "server.malachai.io",
            'port': 10000,
            'username': "",
            'password': "",
            'database': "securities",
        }
        curkey = query_hive_current_sequence(
            **hive_conn,
            table="DIM_ACCOUNTS",
            column="ACCOUNT_KEY"
        )
        command_hive(
            **hive_conn,
            hql="""
            INSERT INTO securities.DIM_ACCOUNTS
            SELECT
                SEQ_ROW_NUM()+{},
                ACCOUNT_NUMBER,
                ACCOUNT_STATUS,
                ACCOUNT_TYPE,
                ACCOUNT_NAME,
                ACCOUNT_OPEN_DATE,
                ACCOUNT_CLOSE_DATE,
                ACCOUNT_DEPOSIT,
                ACCOUNT_WITHDRAWING,
                CREATION_TIMESTAMP,
                NULL,
                TRUE
            FROM securities.TEMP_DIM_ACCOUNTS_INSERTED""".format(curkey)
        )

        # updated
        command_hive(
            **hive_conn,
            hql="""
            UPDATE securities.DIM_ACCOUNTS
            SET ROW_INDICATOR = FALSE,
                ROW_EXPIRATION_TIMESTAMP = CURRENT_TIMESTAMP
            WHERE ACCOUNT_NUMBER IN (
                SELECT ACCOUNT_NUMBER FROM securities.TEMP_DIM_ACCOUNTS_UPDATED
                ) AND ROW_INDICATOR = TRUE"""
        )

        curkey = query_hive_current_sequence(
            **hive_conn,
            table="DIM_ACCOUNTS",
            column="ACCOUNT_KEY"
        )
        command_hive(
            **hive_conn,
            hql="""
            INSERT INTO securities.DIM_ACCOUNTS
            SELECT
                SEQ_ROW_NUM()+{},
                ACCOUNT_NUMBER,
                ACCOUNT_STATUS,
                ACCOUNT_TYPE,
                ACCOUNT_NAME,
                ACCOUNT_OPEN_DATE,
                ACCOUNT_CLOSE_DATE,
                ACCOUNT_DEPOSIT,
                ACCOUNT_WITHDRAWING,
                UPDATE_TIMESTAMP,
                NULL,
                TRUE
            FROM securities.TEMP_DIM_ACCOUNTS_UPDATED""".format(curkey)
        )

    @task()
    def fact_registers_extract(**context):
        registers.extract(context)

    @task()
    def fact_registers_transform(**context):
        registers.transform(context)

    @task()
    def fact_registers_load():
        registers.load()

    # Dimension tables
    dim_u_ex = dim_users_extract()
    dim_u_tf = dim_users_transform()
    dim_u_ld = dim_users_load()

    dim_a_ex = dim_accounts_extract()
    dim_a_tf = dim_accounts_transform()
    dim_a_ld = dim_accounts_load()

    dim_u_ex >> dim_u_tf >> dim_u_ld
    dim_a_ex >> dim_a_tf >> dim_a_ld

    # Fact tables
    fact_r_ex = fact_registers_extract()
    fact_r_tf = fact_registers_transform()
    fact_r_ld = fact_registers_load()

    dim_u_ld >> fact_r_ex >> fact_r_tf >> fact_r_ld


etl_dag()