#=========== IMPORT PYTHON LIBRARY ===========
from airflow import DAG, utils
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.models import Variable
import pendulum
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.jdbc_hook import JdbcHook


'''
cbs_loan_diary_uat
{
  "net_dir": "/apps/batch/NetRev2/",
  "project_dir": "/home/hdpsvc/airflow/dags/jobs/cbs_loan_diary_uat/",
  "schedule_interval": "0 6 * * *",
  "sql_path": "/home/hdpsvc/airflow/dags/hql/cbs_loan_diary_uat/"
}
'''

#=========== DECLARE & SET VARIABLE AIRFLOW ===========
driver = Variable.get("conf_driver", deserialize_json=True)
jars = driver["jars"]
env = driver["env"]
spark = Variable.get("conf_spark", deserialize_json=True)
param = Variable.get("cbs_loan_diary_uat", deserialize_json=True)
net_dir = param["net_dir"]
project_dir = param["project_dir"]
sql_path = param["sql_path"]
schedule_interval = param["schedule_interval"]
email_receiver = ['IT.DataEngineering@Intra.BTPN','IT.DataOperations@Intra.BTPN','IT.DCO@Intra.BTPN']
local_tz = pendulum.timezone("Asia/Jakarta")
date_today = datetime.now()

#=========== SET ARGS ===========

args = {
    'owner': 'simon, Amel',
    # 'provide_context': True,
    'depends_on_past': True,
    'start_date': datetime(2024,5,29, tzinfo=local_tz),
    'end_date': datetime(2024,5,30, tzinfo=local_tz),
    'email':email_receiver,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

#=========== TABLE LIST ===========
table_list = [
    {'uid': 1, 'table_name' : 'stg_loan_diary'}#,
]

#=========== SET DAG ===========

dag = DAG(
        dag_id='cbs_loan_diary_to_up_uat',
        description='ods to DB loan_diary (UAT)',
        default_args=args,
        schedule_interval=schedule_interval,
        catchup=False,
        concurrency=2,
        max_active_runs=1,
        tags=['cbs']
)

#=========== SET DUMMY TASK ===========

start = DummyOperator(
    task_id='start',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

# #======= SENSING External DAG ======
# loaniq_sensing = ExternalTaskSensor(
#     task_id='loaniq_sensing',
#     execution_date_fn=lambda dt: dt - timedelta(hours=1, minutes=30),
#     external_dag_id='cbs_loaniq_to_ods',
#     external_task_id='end',
#     mode='reschedule',
#     poke_interval=60*10,
#     timeout=60*60*6,
#     dag=dag
# )

doka_sensing = ExternalTaskSensor(
    task_id='doka_sensing',
    execution_date_fn=lambda dt: dt - timedelta(hours=1, minutes=30),
    external_dag_id='cbs_tradefinance_to_ods',
    external_task_id='end',
    mode='reschedule',
    poke_interval=60*10,
    timeout=60*60*6,
    dag=dag
)

eq_sensing = ExternalTaskSensor(
    task_id='eq_sensing',
    execution_date_fn=lambda dt: dt - timedelta(hours=5),
    external_dag_id='ods_to_staging_and_history_data_metric_sm6',
    external_task_id='wait',
    mode='reschedule',
    poke_interval=60*10,
    timeout=60*60*6,
    dag=dag
)

# =========== SET EMAIL START/END NOTIFICATION ===========
def email_start(**context):
    buss_date = context['tomorrow_ds']
    print("""
        subject : ETL {0} is Started.<br/><br/>
        Message :
            Dear All, <br/><br/>

            ETL Daily {0} for Business Date {1} is Started.
    """.format(dag.description, buss_date))
    # email_op = EmailOperator(
    #     task_id='send_email',
    #     to=email_receiver,
    #     subject="ETL {0} is Started.".format(dag.description),
    #     html_content="""
    #         Dear All, <br/><br/>
    #
    #         ETL Daily {0} for Business Date {1} is Started.
    #         """.format(dag.description, buss_date),
    # )
    # email_op.execute(context)

op_email_start = PythonOperator(
    task_id='email_start',
    python_callable=email_start,
    provide_context=True,
    dag=dag
)

def email_end(**context):
    buss_date = context['tomorrow_ds']
    print("""
        subject : ETL {0} is Finished.<br/><br/>
        Message :
            Dear All, <br/><br/>

            ETL Daily {0} for Business Date {1} is Finished.
    """.format(dag.description, buss_date))
    # email_op = EmailOperator(
    #     task_id='send_email',
    #     to=email_receiver,
    #     subject="ETL {0} is Finished.".format(dag.description),
    #     html_content="""
    #         Dear All, <br/><br/>
    #
    #         ETL Daily {0} for Business Date {1} is Finished.
    #         """.format(dag.description, buss_date),
    # )
    # email_op.execute(context)

op_email_end = PythonOperator(
    task_id='email_end',
    python_callable=email_end,
    provide_context=True,
    dag=dag
)

# =========== SET LOG START/END JOB =============
def spark_job(task_id, application, sql_file, sql_path, *extra_args, **kwargs):
    job_time = str(date_today)
    return SparkSubmitOperator(
        task_id=task_id,
        application=str(application),
        name='{{ task_instance_key_str }}',
        application_args=[env, '{{ tomorrow_ds }}', sql_file, sql_path, job_time, *extra_args],
        jars=jars,
        conn_id='spark_default',
        conf=spark,
        dag=dag,
        **kwargs,
    )

job_start = spark_job('job_start', project_dir + f'monitor.py', 'monitor_job_start.sql', sql_path)
job_end = spark_job('job_end', project_dir + f'monitor.py', 'monitor_job_finish.sql', sql_path)


def send_error_email(**context):
    hookhive = JdbcHook(jdbc_conn_id='hive_jdbc')
    buss_date = context['tomorrow_ds']
    table_name = context['params']['table_name']
    add_query = context['params']['add_query']

    qry_count = "select count(1) from " + table_name + add_query.format(buss_date)
    print(qry_count)
    count_row = hookhive.get_records(sql=qry_count)
    if(count_row[0][0]==0):
        print("""subject : [Error] ETL {0} is NOT generating any data
                Email :
                    <h3>Table {1} for Business Date {2} is Empty, please check DAG {0}</h3>""".format(dag.description, table_name, buss_date))
        email_op = EmailOperator(
            task_id='send_email',
            to=email_receiver,
            subject="[Error] ETL {0} is NOT generating any data".format(dag.description),
            html_content="""<h3>Table {0} for Business Date {1} is Empty, please check DAG {2}</h3>""".format(table_name, buss_date, dag.description)
        )
        email_op.execute(context)
        raise ValueError("RETURN ZERO ROW")
    else :
        print("count_row : "+str(count_row[0][0]))

#=========== SET LOOP TASK SOURCE2LANDING NET ===========
for i in table_list:
    table_name = i['table_name']
    period_string = "{{ tomorrow_ds }}"

    ods2stg = BashOperator(
        task_id='ods2_' + table_name,
        bash_command='sh ' + net_dir + 'run.sh -m ods2staging -q ' + table_name + '.sql -t ' + table_name + ' -d ' + period_string + ' -c 0 -l 0 -s 0 -e 0 -g ' + project_dir,
        execution_timeout=timedelta(minutes=60),
        dag=dag
    )

    countCheck = PythonOperator(
        task_id=f"count_check_{table_name}",
        python_callable=send_error_email,
        params={'table_name': 'stg_loan_diary_uat.' + table_name, 'add_query': ''},
        provide_context=True,
        dag=dag
    )

    cleanup = MsSqlOperator(
        task_id='Cleanup_' + table_name,
        mssql_conn_id='loan_diary_conn',
        sql='TRUNCATE TABLE bes.dbo.' + table_name,
        dag=dag
    )

    ods2jdbc = BashOperator(
        task_id='jdbc_' + table_name,
        bash_command='sh ' + net_dir + 'run.sh -m ods2jdbc -q ' + table_name + '_jdbc.sql -t ' + table_name + ' -d 0 -c 0 -l 0 -s 0 -e 0 -g ' + project_dir,
        execution_timeout=timedelta(minutes=60),
        dag=dag
    )

#=========== SET WORKFLOW ===========
    # [loaniq_sensing,doka_sensing,eq_sensing] >> start >> op_email_start >> job_start >> ods2stg >> countCheck >> cleanup >> ods2jdbc >> job_end >> op_email_end >> end
    [doka_sensing,eq_sensing] >> start >> op_email_start >> job_start >> ods2stg >> countCheck >> cleanup >> ods2jdbc >> job_end >> op_email_end >> end
