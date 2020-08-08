# Airflow automation

## Problem

* Data analysis requires pipeline of getting data, cleaning data, training model, making prediction, visualizing data, reporting and repeat. We want to automate an entire process and be productive.

## Solution

* Use Airflow (https://airflow.apache.org/) to set dependency and set time schedule.
* Airflow allows you to only use python to specify what tasks you want to automate, when to be executed and set dependency for each taks. The example is below.

![image_01](https://github.com/yukikitayama/airflow-automation/blob/master/05_images/image_01.png)

* Airflow also has a nice user interface, so it's easy to tell what automation is currently running and their status.

![image_02](https://github.com/yukikitayama/airflow-automation/blob/master/05_images/image_02.png)

* Even though you have multiple files to execute, the next task can wait for the previous task to complete its task in the automation like below.

![image_03](https://github.com/yukikitayama/airflow-automation/blob/master/05_images/image_03.png)

## Process

1. Set up EC2 for always running server and RDS to store data.
2. Jupyter Notebook for analysis and visualization in Jupyter Lab.
3. TensorFlow to train neural network to automate developing model and model update.
4. Papermill to execute all the Notebooks.
5. Airflow to set dependency between the Notebooks and set time schedule.
6. Email python package to report out.

* Our goal is to train neural network regression every day, make forecast, and send emails to check whether our forecast is good.

![image_04](https://github.com/yukikitayama/airflow-automation/blob/master/05_images/image_04.png)
