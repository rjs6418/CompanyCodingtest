from pyspark.sql import SparkSession

MAX_MEMORY = "4g"
spark = SparkSession.builder.appName("csv2parquet")\
    .config("spark.executor.memory", MAX_MEMORY)\
    .config("spark.driver.memory", MAX_MEMORY)\
    .getOrCreate()

event_directory = "/Users/kuno/code/iag/csv_data"
event_ratings_file = "event_csv"
event_columns = ["identity_adid", "os", "model", "country", "event_name", "log_id", "server_datetime", "quantity", "price"]
event_df = spark.read.csv(f"file:///{event_directory}/{event_ratings_file}", header=False)
event_df = event_df.toDF(*event_columns)
event_parquet_directory = "/Users/kuno/code/iag/parquet_data/event"
event_df.write.format("parquet").mode("overwrite").save(f"{event_parquet_directory}")

event_df.show()

attribution_directory = "/Users/kuno/code/iag/csv_data"
attribution_file = "attribution_csv"
attribution_columns = ["partner", "campaign", "server_datetime", "tracker_id", "log_id", "attribution_type", "identity_adid"]
attribution_df = spark.read.csv(f"file:///{attribution_directory}/{attribution_file}", header=False)
attribution_df = attribution_df.toDF(*attribution_columns)
attributon_parquet_directory = "/Users/kuno/code/iag/parquet_data/attribution"
attribution_df.write.format("parquet").mode("overwrite").save(f"{attributon_parquet_directory}")

attribution_df.show()





