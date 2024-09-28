from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

from config import config


def main():
    spark = SparkSession.builder.appName("SmartCarStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", config.get('AWS_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", config.get('AWS_SECRET_KEY')) \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()

    # Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel('WARN')

    # vehicle schema
    vehicle_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True),
    ])

    # gps_schema
    gps_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])

    # traffic_schema
    traffic_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True)
    ])

    # weather_schema
    weather_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])

    # emergency_schema
    emergency_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes')
                )

    def stream_writer(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())

    vehicle_df = read_kafka_topic('vehicle_data', vehicle_schema).alias('vehicle')
    gps_df = read_kafka_topic('gps_data', gps_schema).alias('gps')
    traffic_df = read_kafka_topic('traffic_data', traffic_schema).alias('traffic')
    weather_df = read_kafka_topic('weather_data', weather_schema).alias('weather')
    emergency_df = read_kafka_topic('emergency_data', emergency_schema).alias('emergency')

    # #join all the dataframes with id and timestamp
    query1 = stream_writer(vehicle_df, 's3a://spark-streaming-data/checkpoints/vehicle_data',
                 's3a://spark-streaming-data/data/vehicle_data')
    query2 = stream_writer(gps_df, 's3a://spark-streaming-data/checkpoints/gps_data',
                 's3a://spark-streaming-data/data/gps_data')
    query3 = stream_writer(traffic_df, 's3a://spark-streaming-data/checkpoints/traffic_data',
                 's3a://spark-streaming-data/data/traffic_data')
    query4 = stream_writer(weather_df, 's3a://spark-streaming-data/checkpoints/weather_data',
                 's3a://spark-streaming-data/data/weather_data')
    query5 = stream_writer(emergency_df, 's3a://spark-streaming-data/checkpoints/emergency_data',
                 's3a://spark-streaming-data/data/emergency_data')

    query5.awaitTermination()

if __name__ == "__main__":
    main()
