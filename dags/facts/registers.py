from datetime import datetime

from utils.delivery import extract_mysql_to_hdfs, query_hive_current_sequence, command_hive

def extract(context):
    logical_date = context['ds_nodash']
    if 'logical_date' in context['params'] and context['params']['logical_date'] is not None:
        logical_date = context['params']['logical_date']
    conn_args = {
        'mysql_host': "localhost",
        'mysql_user': "malachai",
        'mysql_pass': "0107",
        'mysql_db': "securities",
        'hdfs_host': "http://server.malachai.io",
        'hdfs_port': "9870"
    }
    hdfs_dir = "/user/airflow/etl/securities/" + logical_date + "/fact_registers"

    # Extract
    extract_mysql_to_hdfs(
        **conn_args,
        query="""
                            SELECT user_id, DATE_FORMAT(creation_timestamp, '%Y%m%d') AS creation_date FROM users
                            WHERE creation_timestamp >= DATE_ADD(STR_TO_DATE('{}', '%Y%m%d'), INTERVAL -1 DAY);
                            """.format(logical_date),
        hdfs_dir=hdfs_dir,
        hdfs_file=datetime.now().strftime("%Y%m%dT%H%M%S") + ".csv"
    )

def transform(context):
    logical_date = context['ds_nodash']
    if 'logical_date' in context['params'] and context['params']['logical_date'] is not None:
        logical_date = context['params']['logical_date']
    hdfs_dir = "/user/airflow/etl/securities/" + logical_date + "/fact_registers"

    # csv to external table
    hive_conn = {
        'host': "server.malachai.io",
        'port': 10000,
        'username': "hive",
        'password': "hive",
        'database': "securities",
    }
    command_hive(
        **hive_conn,
        hql="""
                ALTER TABLE securities.TEMP_FACT_REGISTERS
                SET LOCATION 'hdfs://{}'
                """.format(hdfs_dir)
    )

def load():
    hive_conn = {
        'host': "server.malachai.io",
        'port': 10000,
        'username': "hive",
        'password': "hive",
        'database': "securities",
    }
    command_hive(
        **hive_conn,
        hql="""
                INSERT INTO securities.FACT_REGISTERS
                SELECT DC.CALENDER_KEY, DU.USER_KEY FROM securities.TEMP_FACT_REGISTERS AS TFR
                JOIN securities.DIM_CALENDERS DC
                    ON TFR.REGISTER_DATE = DC.CALENDER_DATE
                JOIN (
                    SELECT USER_KEY, USER_NUMBER FROM securities.DIM_USERS
                    WHERE ROW_INDICATOR=True) DU
                    ON TFR.USER_NUMBER = DU.USER_NUMBER"""
    )