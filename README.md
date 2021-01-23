# Processing data files by reading and writing in S3 using with Spark EMR and S3 AWS.

>> We created a EMR cluster 
>> assigned the keypair and 
>> assigned the corresponding access Keys in the CLI AWS setup

>> assigned KEY and SECRET KEY paramenters to allow user and role access S3 and load data into Redshift in the cluster.

>> Submitted ETL application to EMR. 


> We are creating the Spark application 

>>def create_spark_session():
>>    spark = SparkSession \
>>    .builder \
>>    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
>>    .getOrCreate()
>>    return spark
# We are creating the following tables to get started:

## 1 Fact Table
> songplays - records in event data associated with song plays i.e. records with page NextSong
>> songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## 2 Dimension Tables
> users - users in the app
>> user_id, first_name, last_name, gender, level
> songs - songs in music database
>> song_id, title, artist_id, year, duration
> artists - artists in music database
>> artist_id, name, location, lattitude, longitude
> time - timestamps of records in songplays broken down into specific units
>> start_time, hour, day, week, month, year, weekday


# ETL Process
## Processing the song data - created the song and artist tables

 >>   # get filepath to song data file
 >>   song_data = "s3n://udacity-dendl/song_data/*/*/*/*.json"
    
 >>   # read song data file
 >>   df_songs_data = spark.read.json(song_data)
 >>   df_songs_data.persist()

 >>   # extract columns to create songs table
    
 >>   df_songs_data.createOrReplaceTempView("songs_data_table")
    
 >>   # create song table.
 >>   songs_table = spark.sql("""
 >>                           SELECT DISTINCT song_id, 
 >>                           title,
 >>                           artist_id,
 >>                           year,
 >>                           duration
 >>                           FROM songs_data_table
 >>                           WHERE song_id IS NOT NULL
 >>                       """)
 
 
## Write files in parquet AND partition the song data file INTO S3 data file

 >>   # write  songs table to parquet files partitioned by year and artist
 >>   songs_table.write.partitionBy("year","artist_id").mode('overwrite').parquet(os.path.join(output_data, 'songs'))

## Processing the log data - created the users and time tables and creating parquet files

 >> def process_log_data(spark, input_data, output_data):

 >>   """
 >>   Log data being processed -> read and written into S3

 >>   """
 >>   # get filepath to log data file
 >>   log_data = "s3n://udacity-dendl/log-data/*.json"

 >>   # read log data file
 >>   df_log_data = spark.read.json(log_data)
 >>   df_log_data.persist()
    
 >>   # filter by actions for song plays
 >>   df_log_data = df_log_data.filter(df_log_data.page == 'NextSong')
    
 >>   # creating log_data_table
 >>   df_log_data.createOrReplaceTempView("log_data_table")

 >>   # extract columns for users table    
   
 >>   users_table = spark.sql("""
 >>                           SELECT DISTINCT userId as user_id, 
 >>                           firstName as first_name,
 >>                           lastName as last_name,
 >>                           gender as gender,
 >>                           level as level
 >>                           FROM log_data_table
 >>                           WHERE userId IS NOT NULL
 >>                       """)
    
 >>   # write users table to parquet files
 >>   users_table.write.parquet(os.path.join(output_data, "users/") , mode="overwrite")

## Write files in parquet AND partition the log data file INTO S3 data file

>>    # write time table to parquet files partitioned by year and month
>>    time_table.write.partitionBy("year","month").parquet(output_data + "time_table.parquet", mode = "overwrite")

