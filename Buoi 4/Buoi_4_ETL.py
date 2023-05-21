import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


class ETL_daily():
    def __init__(self, spark):
        self.spark = spark

    def load_data(self, read_path, file_name):
        print('------------------------------')
        print('Read data from HDFS')
        self.df = self.spark.read.json(read_path + file_name + '.json')
        # print('------------------------------')
        # print('Showing data structure')
        # print('------------------------------')
        # self.df.printSchema()

    def transform_data(self, date):
        self.df = self.df.select('_source.*')
        print('------------------------------')
        print('Transforming data')
        print('------------------------------')
        # Transform data to desired form
        self.df = self.df.withColumn("Type",when((col("AppName") == 'CHANNEL') | (col("AppName") =='DSHD')| (col("AppName") =='KPLUS')| (col("AppName") =='KPlus'), "Truyền Hình")
        .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS_RES')| (col("AppName") =='BHD_RES')| 
            (col("AppName") =='VOD_RES')| (col("AppName") =='FIMS')| (col("AppName") =='BHD')| (col("AppName") =='DANET'), "Phim Truyện")
        .when((col("AppName") == 'RELAX'), "Giải Trí")
        .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
        .when((col("AppName") == 'SPORT'), "Thể Thao")
        .otherwise("Error"))

        # Select necessary columns
        self.df = self.df.select('Contract','Type','TotalDuration')
    
        # Select non_None row
        self.df = self.df.filter(self.df['Contract'] != '0' )
        self.df = self.df.filter(self.df['Type'] != 'Error')

        print('------------------------------')
        print('Pivoting data')
        # Pivot data
        self.result = self.df.groupBy('Contract', 'Type').pivot('Type').sum('TotalDuration')
        
        # Add date column
        self.result = self.result.withColumn('Date', lit(date))
        return self.result

    def save_data(self, save_path, file_name):
        # Saving output
        print('------------------------------')
        print('Saving result output')
        # Transform to Pandas Dataframe 
        self.result = self.result.select("*").toPandas()
        self.result.to_csv(save_path + file_name + '.csv', index=False)

        print('------------------------------')
        print(f'Saving {file_name}.csv')
        return print("Task Ran Successfully!!!!")

