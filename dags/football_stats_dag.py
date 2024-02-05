import pendulum
from airflow import DAG
from airflow.decorators import dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from jinja2 import Environment
from datetime import datetime


def normaling_datetime(ts_nodash):
    print(ts_nodash, type(ts_nodash))
    date = datetime.strptime(ts_nodash, '%Y%m%dT%H%M%S')
    return str(date.strftime('%Y-%m-%d %H:%M:%S'))

def get_column_from_table(path_sql):
        postgres_hook = PostgresHook(postgres_conn_id="postgres-test")
        conn = postgres_hook.get_conn()
        sql = ''
        with open(path_sql) as ps:
              for line in ps:
                    sql += line
        cursor = conn.cursor()
        cursor.execute(sql)
        res = []
        for line in cursor:
            res+= line
        return ','.join(res)

def insert_data_with_J2_params(path_sql, table_name, columns, values):
        phook = PostgresHook(postgres_conn_id="postgres-test")
        conn = phook.get_conn()
        presql = ''
        with open(path_sql) as ps:
              for line in ps:
                    presql+= line
        env = Environment()
        template = env.from_string(presql)
        sql = template.render(table_name = table_name, columns = columns, values = values)
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        conn.close()

def set_data_in_database(**kwargs ):
      with open(kwargs['path_to_dataset'], 'r', encoding='latin-1') as ds:
            values = ''
            date =  normaling_datetime(kwargs['date'])
            columns = get_column_from_table('/opt/airflow/dags/sql/get_column_fs.sql')
            next(ds)
            rows = 0
            for line in ds:
                if rows < 1000:
                    prevalue = line.replace('\'','@@').replace(';;', ';~~~;').rstrip().split(';')
                    prevalue.append(date)
                    values += str(tuple(prevalue)) + ','
                    rows += 1
                if rows == 1000:
                        insert_data_with_J2_params("sql/insert_into_table.sql",'f_stats', columns, values.replace('@@','\'\'').replace('\'~~~\'', 'null').rstrip(','))
                        rows = 0
                        values = ''
            if rows > 0 :
                 insert_data_with_J2_params('sql/insert_into_table.sql','f_stats', columns, values.replace('@@','\'\'').replace('\'~~~\'', 'null').rstrip(','))

def telegram_alert(self):
     from telebot import Telegram_log as tl
     from airflow.models import Variable


     token = Variable.get("tgm_tok")
     chat_id = Variable.get("tgm_chat_id")
     tl(token, chat_id).logger('fbstat_allert')


dag = DAG(
    dag_id = "football_players_stats_processing",
    start_date = pendulum.datetime(2024, 2, 3),
    schedule_interval = "*/30 * * * * ",
    catchup=True,
    tags = ["test"]
)
create_fbstats_table = SQLExecuteQueryOperator(
      task_id = 'create_football_stats_table_idexists', 
      conn_id = "postgres-test", 
      sql = 'sql/create_table.sql', 
      dag=dag)

insert_into_table = PythonOperator(
     task_id = 'insert',
     python_callable = set_data_in_database,
     on_failure_callback = telegram_alert,
     provide_context = True,
     op_kwargs = { "path_to_dataset" : "dataset/2021-2022FootballPlayerStats.csv",
          "date" : "{{ts_nodash}}"},
     dag=dag
    )

create_fb_nation_stats = SQLExecuteQueryOperator(
      task_id = 'create_football_stats_for_nation_table_idexists',
      conn_id = "postgres-test",
      sql = 'sql/create_table_ngs.sql',
      dag=dag)

insert_nations_stats = SQLExecuteQueryOperator(
      task_id = 'insert_nat_stats',
      conn_id = "postgres-test",
      sql = 'sql/insert_nation_goals.sql',
      dag=dag)

create_fbstats_table >> insert_into_table >> insert_nations_stats

create_fb_nation_stats >> insert_nations_stats