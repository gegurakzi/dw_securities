from datetime import datetime

from utils.delivery import extract_mysql_to_hdfs, query_hive_current_sequence, command_hive

def extract(context):
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
    hdfs_dir_insert = "/user/airflow/etl/securities/" + logical_date + "/dim_users_inserted"
    hdfs_dir_update = "/user/airflow/etl/securities/" + logical_date + "/dim_users_updated"

    # Extract
    extract_mysql_to_hdfs(
        **conn_args,
        query="""
                SELECT user_id, name, status, type, email, phone_number, gender, age, birth_date, income, risk_tolerance, creation_timestamp, update_timestamp FROM users
                WHERE creation_timestamp >= DATE_ADD(STR_TO_DATE('{}', '%Y%m%d'), INTERVAL -1 DAY);
                """.format(logical_date),
        hdfs_dir=hdfs_dir_insert,
        hdfs_file=datetime.now().strftime("%Y%m%dT%H%M%S") + ".csv"
    )
    extract_mysql_to_hdfs(
        **conn_args,
        query="""
                SELECT user_id, name, status, type, email, phone_number, gender, age, birth_date, income, risk_tolerance, creation_timestamp, update_timestamp FROM users
                WHERE creation_timestamp < DATE_ADD(STR_TO_DATE('{}', '%Y%m%d'), INTERVAL -1 DAY)
                    AND update_timestamp >= DATE_ADD(STR_TO_DATE('{}', '%Y%m%d'), INTERVAL -1 DAY);
                """.format(logical_date, logical_date),
        hdfs_dir=hdfs_dir_update,
        hdfs_file=datetime.now().strftime("%Y%m%dT%H%M%S") + ".csv"
    )

def transform(context):
    logical_date = context['ds_nodash']
    if 'logical_date' in context['params'] and context['params']['logical_date'] is not None:
        logical_date = context['params']['logical_date']
    hdfs_dir_insert = "/user/airflow/etl/securities/" + logical_date + "/dim_users_inserted"
    hdfs_dir_update = "/user/airflow/etl/securities/" + logical_date + "/dim_users_updated"

    # insert.csv to external table
    hive_conn = {
        'host': "",
        'port': 10000,
        'username': "",
        'password': "",
        'database': "securities",
    }
    command_hive(
        **hive_conn,
        hql="""
                ALTER TABLE securities.TEMP_DIM_USERS_INSERTED
                SET LOCATION 'hdfs://{}'
                """.format(hdfs_dir_insert)
    )
    # update.csv to external table
    command_hive(
        **hive_conn,
        hql="""
                ALTER TABLE securities.TEMP_DIM_USERS_UPDATED
                SET LOCATION 'hdfs://{}'
                """.format(hdfs_dir_update)
    )

def load(context):
    # inserted
    hive_conn = {
        'host': "",
        'port': 10000,
        'username': "",
        'password': "",
        'database': "securities",
    }
    curkey = query_hive_current_sequence(
        **hive_conn,
        table="DIM_USERS",
        column="USER_KEY"
    )
    command_hive(
        **hive_conn,
        hql="""
                INSERT INTO securities.DIM_USERS
                SELECT
                    SEQ_ROW_NUM()+{},
                    USER_NUMBER,
                    USER_NAME,
                    USER_STATUS,
                    USER_TYPE,
                    USER_EMAIL,
                    USER_PHONE,
                    USER_GENDER,
                    USER_AGE,
                    USER_BIRTH_DATE,
                    USER_INCOME,
                    USER_RISK_TOLERANCE,
                    CREATION_TIMESTAMP,
                    NULL,
                    TRUE
                FROM securities.TEMP_DIM_USERS_INSERTED""".format(curkey)
    )

    # updated
    command_hive(
        **hive_conn,
        hql="""
                UPDATE securities.DIM_USERS
                SET ROW_INDICATOR = FALSE,
                    ROW_EXPIRATION_TIMESTAMP = CURRENT_TIMESTAMP
                WHERE USER_NUMBER IN (
                    SELECT USER_NUMBER FROM securities.TEMP_DIM_USERS_UPDATED
                    ) AND ROW_INDICATOR = TRUE"""
    )

    curkey = query_hive_current_sequence(
        **hive_conn,
        table="DIM_USERS",
        column="USER_KEY"
    )
    command_hive(
        **hive_conn,
        hql="""
                INSERT INTO securities.DIM_USERS
                SELECT
                    SEQ_ROW_NUM()+{},
                    USER_NUMBER,
                    USER_NAME,
                    USER_STATUS,
                    USER_TYPE,
                    USER_EMAIL,
                    USER_PHONE,
                    USER_GENDER,
                    USER_AGE,
                    USER_BIRTH_DATE,
                    USER_INCOME,
                    USER_RISK_TOLERANCE,
                    UPDATE_TIMESTAMP,
                    NULL,
                    TRUE
                FROM securities.TEMP_DIM_USERS_UPDATED""".format(curkey)
    )