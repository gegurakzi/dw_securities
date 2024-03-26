from hdfs.client import InsecureClient
import pymysql
from pyhive import hive
import pandas as pd

def extract_mysql_to_hdfs(
        mysql_host="",
        mysql_user="",
        mysql_pass="",
        mysql_db="",
        hdfs_host="",
        hdfs_port="",
        query="",
        hdfs_dir="",
        hdfs_file="",
        file_encoding="utf-8"
):
    hdfs_client = InsecureClient(hdfs_host + ":" + hdfs_port)

    with pymysql.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_pass,
            db=mysql_db
    ) as conn:
        cur = conn.cursor()
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall())

    with hdfs_client.write(hdfs_dir+"/"+hdfs_file, encoding=file_encoding, ) as writer:
        df.to_csv(writer, index=False)

def query_hive_current_sequence(
        host="",
        port="",
        username="",
        password="",
        database="",
        table="",
        column=""
):
    with hive.Connection(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        auth='LDAP'
    ) as hive_con:
        cursor = hive_con.cursor()
        cursor.execute("""SELECT MAX({}) FROM {}.{}""".format(column, database, table))
        curkey = cursor.fetchall()[0][0]
        if curkey == None: curkey = 0
    return curkey

def command_hive(
        host="",
        port="",
        username="",
        password="",
        database="",
        hql=""
):
    with hive.Connection(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        auth='LDAP'
    ) as hive_con:
        cursor = hive_con.cursor()
        cursor.execute(hql)
        hive_con.commit()