# -*- coding: utf-8 -*-

#Import statements

from __future__ import print_function

import time
from datetime import datetime
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

default_args = {
	'owner':'vimal',
	'start_date': days_ago(2),
	
}
	
dag=DAG(
	dag_id='example_python_operator',
	default_args=default_args,
	schedule_interval="@once",
	)
	
def print_content(ds,**kwargs):
	
	dateTimeObj = datetime.now()
	print('timestamp is ',dateTimeObj)
	test=5
	raise ValueError('File not parsed completely/correctly')
		
	return "Heyyyyyyyyyyyyyy I passed"

	
task1=PythonOperator(
	task_id= "print_the_content",
	provide_context=True,
	python_callable=print_content,
	dag=dag,
	)
	
def my_Sleeping_funciton(random_base):
	
	time.sleep(random_base)
	
for i in range(5):
	task=PythonOperator(
		task_id='sleep_for_' + str(i),
		python_callable=my_Sleeping_funciton,
		op_kwargs={'random_base': float(i)/10},
		dag=dag,
		
		)
		
	task1>>task
	
	
	