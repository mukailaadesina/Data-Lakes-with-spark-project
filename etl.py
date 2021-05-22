import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from zipfile import ZipFile


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    Processes all song data JSON files in the given input folder and stores them in parquet format in the output folder.
    :param spark: spark session
    :param input_data: input data path (from s3)
    :param output_data: output data path (to s3)
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # for subset of song_data
    #song_data = input_data + 'song_data/A/A/A/*.json'
    
    # this is to define the song schema
    songSchema = StructType([
        StructField('num_songs', IntegerType()),
        StructField('artist_id', StringType()),
        StructField('artist_latitude', DoubleType()),
        StructField('artist_longitude', DoubleType()),
        StructField('artist_location', StringType()),
        StructField('artist_name', StringType()),
        StructField('song_id', StringType()),
        StructField('title', StringType()),
        StructField('duration', DoubleType()),
        StructField('year', IntegerType())
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=songSchema) 
    
    # print Schema and show five(5) rows
    df.printSchema
    df.show(5)

    # extract columns to create songs table
    song_fields = ["title", "artist_id", "year", "duration"]
    songs_table = songs_table = df.select(song_fields).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data,'parquet/songs'), 'overwrite')

    # extract columns to create artists table
    artists_fields = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude",
                      "artist_longitude as longitude"]
    artists_table =  df.selectExpr(artists_fields).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'parquet/artists'), 'overwrite')
    

def process_log_data(spark, input_data, output_data):
    
    """
    Processes all log data JSON files in the given input folder and stores them in parquet format in the output folder.
    :param spark: spark session
    :param input_data: location of log_data json files with the events data
    :param output_data: S3 bucket were dimensional tables in parquet format
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    
    # read log data file
    df = spark.read.json(log_data)
    
    # print Schema and show five(5) rows
    df.printSchema
    df.show(5)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(users_fields).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'parquet/users'), 'overwrite')

 
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    # extract columns to create time table
    time_table = df.select(
        'timestamp',
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year'),
        date_format('datetime', 'F').alias('weekday')
    )
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'parquet/time'), 'overwrite')
           
    # read in song data to use for songplays table
    df_songs   = spark.read.parquet(output_data + 'parquet/songs/*/*/*')
    df_artists = spark.read.parquet(output_data + 'parquet/artists/*')
    # for subset of song_data
    #df_songs   = spark.read.parquet(output_data + 'parquet/song/A/A/A/*')
    #df_artists = spark.read.parquet(output_data + 'parquet/artists/*')
  
    
    songs_logs = df.join(songs_df, (df.song == songs_df.title))
    artists_songs_logs = songs_logs.join(df_artists, (songs_logs.artist == df_artists.name))

    # extract columns from joined song and log datasets to create songplays table 
    songplays = artists_songs_logs.join(
        time_table,
        artists_songs_logs.ts == time_table.start_time, 'left'
    ).drop(artists_songs_logs.year)
    
    songplays_table = songplays.select(
        col('start_time').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        col('month').alias('month'),
    ).repartition("year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')

def main():
    
    """
        Extract songs and events data from S3, Transform it into dimensional tables format, 
        and Load it back to S3 in Parquet format
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-projlake"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
