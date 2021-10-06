import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

import pandas as pd



def get_data1():
          url1='https://drive.google.com/file/d/1Rc9G_9WiaRRjjBUNx4h-cw2EwXmlC4OO/view?usp=sharing'


          file_id1=url1.split('/')[-2]
          dwn_url1='https://drive.google.com/uc?id=' + file_id1
          order_detail = pd.read_csv(dwn_url1)



          order_detail.to_csv("/home/airflow/data/order_detail.csv", index=False)

def get_data2():
          url2='https://drive.google.com/file/d/1dxAooQO0yXUdw2MqLTo00CP3atLKfOhb/view?usp=sharing'
          file_id2=url2.split('/')[-2]
          dwn_url2='https://drive.google.com/uc?id=' + file_id2
          restaurant_detail = pd.read_csv(dwn_url2)
          restaurant_detail.to_csv("/home/airflow/data/restaurant_detail.csv", index=False)


def clear_order_detail():
          order_detail = pd.read_csv("/home/airflow/data/order_detail.csv")

          order_detail['dt'] = order_detail['order_created_timestamp']
          order_detail["dt"] = order_detail.apply(lambda x: x["dt"].replace("-",""), axis=1)
          order_detail['dt'] = pd.to_datetime(order_detail['dt']).dt.date
          order_detail['dt'] = order_detail['dt'].astype(str)
          order_detail['discount_no_null'] = order_detail['discount']
          order_detail['discount_no_null'].fillna('0', inplace = True)
          order_detail['discount_no_null'] = order_detail['discount_no_null'].astype(float)
          order_detail.drop(order_detail.index[-1], inplace=True)
          # save ไฟล์ CSV
          order_detail.to_csv("/home/airflow/data/order_detail_new.csv", index=False)
          #query
          data=order_detail.describe()
          discount = pd.DataFrame(data)
          discount.to_csv("/home/airflow/data/discount.csv", index=False)

def clear_cooking():
          restaurant_detail = pd.read_csv("/home/airflow/data/restaurant_detail.csv")

          restaurant_detail.loc[(restaurant_detail['esimated_cooking_time'] >= 10) & (restaurant_detail['esimated_cooking_time'] <= 40), 'cooking_bin'] = '1'
          restaurant_detail.loc[(restaurant_detail['esimated_cooking_time'] >= 41) & (restaurant_detail['esimated_cooking_time'] <= 80), 'cooking_bin'] = '2'
          restaurant_detail.loc[(restaurant_detail['esimated_cooking_time'] >= 81) & (restaurant_detail['esimated_cooking_time'] <= 120), 'cooking_bin'] = '3'
          restaurant_detail.loc[(restaurant_detail['esimated_cooking_time'] >= 121),'cooking_bin'] = '4'
          restaurant_detail['cooking_bin'] = restaurant_detail['cooking_bin'].astype(int)

          # save ไฟล์ CSV
          restaurant_detail.to_csv("/home/airflow/data/restaurant_detail_new.csv", index=False)

          #query
          cooking_data=restaurant_detail['cooking_bin'].value_counts()
          cooking = pd.DataFrame(cooking_data)
          cooking.to_csv("/home/airflow/data/cooking.csv", index=False)




default_args = {
    'owner': 'datath',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
          'Pipeline',
          default_args=default_args,
          description='Pipeline for ETL',
          schedule_interval=timedelta(days=1),
)

t0 =PythonOperator(
          task_id="get_data1",
          python_callable=get_data1,
          dag=dag,

)

t1 =PythonOperator(
          task_id="get_data2",
          python_callable=get_data2,
          dag=dag,)

t2 = PythonOperator(
          task_id="clear_order_detail",
          python_callable=clear_order_detail,
          dag=dag,
)

t3 = PythonOperator(
          task_id="clear_cooking",
          python_callable=clear_cooking,
          dag=dag,
)


t0 >> t2,
t1 >> t3