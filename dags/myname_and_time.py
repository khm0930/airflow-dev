import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
import pendulum
import datetime


def print_time_and_name():
    current_time = pendulum.now("Asia/Seoul")  # 한국 시간대 사용
    print(f"현재 시간: {current_time}, 이름: 김현민")

with DAG(
    dag_id="myname_and_time",
    schedule="0 1 * * *",  # 매일 1시에 실행
    start_date=pendulum.datetime(2024, 10, 30, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # PythonOperator 태스크
    print_task = PythonOperator(
        task_id='print_time_and_name_task',
        python_callable=print_time_and_name
    )
    
    # 성공 시 EmailOperator 태스크
    success_email_task = EmailOperator(
        task_id='success_email_task',
        to='rlagusals010@gmail.com',
        subject='[Airflow] DAG 실행 성공',
        html_content='DAG: {{ task_instance.dag_id }}<br> Task: {{ task_instance.task_id }}<br> Execution Time: {{ ts }}<br> 상태: 성공',
        trigger_rule='all_success'  # print_task가 성공할 때만 실행
    )

    # 실패 시 EmailOperator 태스크
    failure_email_task = EmailOperator(
        task_id='failure_email_task',
        to='rlagusals010@gmail.com',
        subject='[Airflow] DAG 실행 실패',
        html_content='DAG: {{ task_instance.dag_id }}<br> Task: {{ task_instance.task_id }}<br> Execution Time: {{ ts }}<br> 상태: 실패',
        trigger_rule='one_failed'  #print_task가 실패하면 이 태스크 실행
    )

    # 실행 순서 정의
    print_task >> [success_email_task, failure_email_task]
