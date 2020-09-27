# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "vimal",
    "depends_on_past": False,
    "start_date": datetime(2019, 11, 1)
}

dag = DAG("Email_operator_example", default_args=default_args, schedule_interval=None)


t1 = EmailOperator(
    task_id="send_mail", 
    to='airflow@example.com',
    subject='Test mail',
    html_content='<p> You have got mail from airflow! <p>',
    dag=dag)
    
    
def error_function():
    raise ValueError('Something wrong')
   
   
t2 = PythonOperator(
    task_id='failing_task',
    python_callable=error_function,
    email_on_failure=True,
    email='airflow@example.com',
    dag=dag,
)

t2>>t1