from datetime import datetime,timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator #from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator  #from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook #from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  #from airflow.hooks.S3_hook import S3Hook
from gclid_linear_attribution_sftp_sql import queries
from tempfile import NamedTemporaryFile
import csv


def connect_to_redshift():
    postgres_conn = PostgresHook(postgres_conn_id='stockx-analytics-datahub').get_conn()
    return postgres_conn


def save_files(records, file_prefix):
    hook = S3Hook('aws_analytics_s3')
    with NamedTemporaryFile(suffix='.csv', mode='w+') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',')
            filewriter.writerow(['Parameters:Attribution Model = linear'])
            filewriter.writerow(['Parameters:TimeZone=America/Detroit'])
            filewriter.writerow(['Google Click ID',
                                 'Conversion Name',
                                 'Conversion Time',
                                 'Attributed Credit',
                                 'Conversion Value',
                                 'Conversion Currency'])
            for row in records:
                filewriter.writerow(row)
            csvfile.seek(0)
            hook.load_file(csvfile.name, file_prefix, 'stockx-analytics-sftp', replace=True)
            csvfile.close()
    return True


def fetch(conn, query, verbose=False):
    if verbose:
        print(query)

    cur = conn.cursor()
    cur.execute(query)
    resp = cur.fetchall()
    desc = cur.description

    if verbose:
        print(desc)

    cur.close()
    return resp, desc

'''
def gclid_file_fetch():
    conn = connect_to_redshift()
    resp, desc = fetch(conn, getattr(queries, 'gclid_sftp_fetch'), verbose=True)
    if len(resp) != 0:
        save_files(resp, 'google_ads/gclid_linear_attribution.csv')
    else:
        print('no records from table')'''


def gclid_all_users_file_fetch():
    conn = connect_to_redshift()
    resp, desc = fetch(conn, getattr(queries, 'gclid_all_users_sftp_fetch'), verbose=True)
    if len(resp) != 0:
        save_files(resp, 'google_ads/gclid_linear_attribution.csv')
    else:
        print('no records from table')


default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 12),
    'email': ['dataengineering@stockx.com', 'deng-incident-astronomer-airflow.yp62p7yx@stockx.pagerduty.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'sla': timedelta(minutes=60)
}


dag = DAG('gclid_linear_attribution_sftp',
          default_args=default_args,
          max_active_runs=1,
          schedule_interval='0 7 * * *',
          description='GCLID Linear Attribution for Google AD Conversion',
          catchup=False,
          tags = ['Customer','GoogleAds','CSV','Redshift'])

'''
t_1 = PostgresOperator(task_id='insert_new',
                       postgres_conn_id='stockx-analytics-datahub',
                       sql=getattr(queries, 'gclid_insert'),
                       dag=dag)


t_2 = PostgresOperator(task_id='gclid_sftp_set',
                       postgres_conn_id='stockx-analytics-datahub',
                       sql=getattr(queries, 'gclid_sftp_set'),
                       dag=dag)


t_3 = PythonOperator(task_id='gclid_fetch_and_upload',
                     python_callable=gclid_file_fetch,
                     dag=dag)


t_4 = PostgresOperator(task_id='gclid_sftp_finish',
                       postgres_conn_id='stockx-analytics-datahub',
                       sql=getattr(queries, 'gclid_sftp_update'),
                       dag=dag)'''


t_5 = PostgresOperator(task_id='insert_all_users',
                       postgres_conn_id='stockx-analytics-datahub',
                       sql=getattr(queries, 'gclid_all_users_insert'),
                       dag=dag)


t_6 = PostgresOperator(task_id='gclid_all_users_sftp_set',
                       postgres_conn_id='stockx-analytics-datahub',
                       sql=getattr(queries, 'gclid_all_users_sftp_set'),
                       dag=dag)


t_7 = PythonOperator(task_id='gclid_all_users_fetch_and_upload',
                     python_callable=gclid_all_users_file_fetch,
                     dag=dag)


t_8 = PostgresOperator(task_id='gclid_all_users_sftp_finish',
                       postgres_conn_id='stockx-analytics-datahub',
                       sql=getattr(queries, 'gclid_all_users_sftp_update'),
                       dag=dag)

'''
t_1.set_downstream(t_2)
t_2.set_downstream(t_3)
t_3.set_downstream(t_4)
'''

t_5.set_downstream(t_6)
t_6.set_downstream(t_7)
t_7.set_downstream(t_8)