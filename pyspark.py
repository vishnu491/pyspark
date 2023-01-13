#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""
# Title : PySpark Datalake
# Description : Reading the file from disk and write to different layers after cleansing and aggregation
# Author : vishnu vardhan reddy
# Date : 13-Jan-2022
# Version : 1.0 (Initial Draft)
# Usage : spark-submit PySpark_datalake.py  
"""


# In[1]:


# import modiles
from datetime import datetime
import time
import pyspark
import sys,logging
import delta
from pyspark.sql import SparkSession  #for starting spark session
from delta.pip_utils import configure_spark_with_delta_pip  
import pyspark.sql.functions as F  #data manipulation
from delta.tables import *  #reading and writing delta tables


# In[5]:


# Logging configuration
formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)


# In[4]:


# current time variable to be used for logging purpose
dt_string = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
# change it to your app name
AppName = "ETL"


# In[5]:


def bronze_data():
    logger.info("Readind the file and writing to Bronze layer")
    bronze = spark.read                   .csv(f'{file_full_path}',header='true',inferSchema='true')                   .write                   .format('delta')                   .save(f'{path}/bronze_data/{time.strftime("%Y%m%d")}')                 


# In[6]:


def silver_data():
    logger.info("Bronze to silver layer")
    df = spark.read.format('delta').load(f'{path}/bronze_data/{time.strftime("%Y%m%d")}') 
    df.dropDuplicates()           .withColumn('my_dest_hash',F.sha2(F.concat_ws(',',*df.columns),256))           .write           .mode('overwrite')           .format('delta')           .save(f'{path}/silver_table')
        


# In[7]:


def gold_data():
    logger.info("Preparing aggreagted data and writing to Gold layer")
    df = spark.read.format('delta').load(f'{path}/silver_table') 
    df.groupBy('type').agg(F.sum('isFraud').alias('Fraud_count'),F.sum('isFlaggedFraud').alias('isFlagged_count'))                       .withColumn('missed',F.col('Fraud_count')-F.col('isFlagged_count'))       .write       .mode('overwrite')       .format('csv')       .save(f'{path}/gold_data/fraud_detected_count.csv')


# In[ ]:


file_full_path = input('please specify file full path : ')


# In[8]:


path = input('please specify the location to write the data : ')


# In[12]:


def spark_session():
    global spark
    spark = configure_spark_with_delta_pip(pyspark.sql.SparkSession.builder.appName("ETL")                      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")                      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")                       .config("spark.executor.memory", "3g")                      .config("spark.driver.memory", "4g")                      .config("spark.executor.instances", 4)                      .config("spark.executor.cores", 4)).getOrCreate()
    logger.info("Starting spark application")


# In[10]:


def main():
    
    spark_session()

    # start spark code
 
    #calling function 1
    bronze_data()

    #calling function 2
    silver_data()
    
    #calling function 3
    gold_data()    

    #do something here
    logger.info("Writing files to bronze,silver,gold is completed")
    logger.info("Ending spark application")
    # end spark code
    spark.stop()
    return None


# In[11]:


if __name__ == '__main__':
    main()
    sys.exit()


# In[ ]:




