import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import Column, udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creating Spark Application
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = "s3n://udacity-dendl/song_data/*/*/*/*.json"
    
    # read song data file
    df_songs_data = spark.read.json(song_data)
    df_songs_data.persist()

    # extract columns to create songs table
    
    df_songs_data.createOrReplaceTempView("songs_data_table")
    
    songs_table = spark.sql("""
                            SELECT DISTINCT song_id, 
                            title,
                            artist_id,
                            year,
                            duration
                            FROM songs_data_table
                            WHERE song_id IS NOT NULL
                        """)

    # write  songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode('overwrite').parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df_songs_data['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = artists_table.dropDuplicates(['artist_id'])
         
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):

    """
    Log data being processed -> read and written into S3

    """
    # get filepath to log data file
    log_data = "s3n://udacity-dendl/log-data/*.json"

    # read log data file
    df_log_data = spark.read.json(log_data)
    df_log_data.persist()
    
    # filter by actions for song plays
    df_log_data = df_log_data.filter(df_log_data.page == 'NextSong')
    
    # creating log_data_table
    df_log_data.createOrReplaceTempView("log_data_table")

    # extract columns for users table    
   
    users_table = spark.sql("""
                            SELECT DISTINCT userId as user_id, 
                            firstName as first_name,
                            lastName as last_name,
                            gender as gender,
                            level as level
                            FROM log_data_table
                            WHERE userId IS NOT NULL
                        """)
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users/") , mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(x) // 1000.0))
    df_log_data = df_log_data.withColumn("timestamp",get_timestamp(df_log_data.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: int(datetime.datetime.fromtimestamp(x / 1000.0).hour))
    df_log_data = df_log_data.withColumn('date', get_datetime(df_log_data.timestamp))
    
    # extract columns to create time table
    time_table = df_log_data.select(Column('timestamp').alias('start_time'),
                           hour('date').alias('hour'),
                           dayofmonth('date').alias('day'),
                           weekofyear('date').alias('week'),
                           month('date').alias('month'),
                           year('date').alias('year'),
                           date_format('date','E').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(output_data + "time_table.parquet", mode = "overwrite")

    # read in song data to use for songplays table
    song_df = spark.read\
                .format("parquet")\
                .option("basePath", os.path.join(output_data, "songs/"))\
                .load(os.path.join(output_data, "songsplays"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_log_data.join(song_df, (df_log_data.song == song_df.title) & (df_log_data.artist == song_df.artist_name) & (df_log_data.length == song_df.duration), 'left_outer')\
        .select(
            df_log_data.timestamp,
            Column("userId").alias('user_id'),
            df_log_data.level,
            song_df.song_id,
            song_df.artist_id,
            Column("sessionId").alias("session_id"),
            df_log_data.location,
            Column("useragent").alias("user_agent"),
            year('datetime').alias('year'),
            month('datetime').alias('month'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data,"songplays_p/", mode = "overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dendl/input/"
    output_data = "s3a://udacity-dendl/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
