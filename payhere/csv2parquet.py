from pyspark.sql import SparkSession
import os

MAX_MEMORY = "4g"
spark = SparkSession.builder.appName("csv2parquet")\
    .config("spark.executor.memory", MAX_MEMORY)\
    .config("spark.driver.memory", MAX_MEMORY)\
    .getOrCreate()
i = 0
folder_Q = os.listdir('./CAI')
for folder_A in folder_Q:
    i += 1
    file_list= os.listdir(f'./CAI/{folder_A}')
    directory = f"/Users/kuno/code/test/payhere/Q2/CAI/{folder_A}"
    for file in file_list:
        if file == ".DS_Store":
            continue
        df = spark.read.csv(f"file:///{directory}/{file}", inferSchema=True, header=True)
        parquet_directory = f"/Users/kuno/code/test/payhere/Q2/CAI_parquet{i}"
        df.write.format("parquet").mode("append").save(f"{parquet_directory}")
        print(file)



                
                