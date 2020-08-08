# Airflow automation

## Problem

* Data analysis requires pipeline of getting data, cleaning data, training model, making prediction, visualizing data, reporting and repeat. We want to automate an entire process and be productive.

## Solution

* Use Airflow (https://airflow.apache.org/) to set dependency and set time schedule.

## Process

1. Set up EC2 for always running server and RDS to store data.
2. Jupyter Notebook for analysis and visualization in Jupyter Lab.
3. TensorFlow to train neural network to automate developing model and model update.
4. Papermill to execute all the Notebooks.
5. Airflow to set dependency between the Notebooks and set time schedule.
6. Email python package to report out.

![image_01](https://github.com/yukikitayama/airflow-automation/blob/master/05_images/image_01.png)

![image_02](https://github.com/yukikitayama/airflow-automation/blob/master/05_images/image_02.png)

![image_03](https://github.com/yukikitayama/airflow-automation/blob/master/05_images/image_03.png)

![image_04](https://github.com/yukikitayama/airflow-automation/blob/master/05_images/image_04.png)
