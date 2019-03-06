import listenbrainz_spark
from listenbrainz_spark import config
from setup import spark
from pyspark.sql import Row

from datetime import datetime
import os, sys


def prepare_user_data(table):
    users_q = spark.sql("""
            SELECT DISTINCT user_name
             From %s
        """ % (table))
    users_rdd = users_q.rdd.zipWithIndex()
    users_rdd = users_rdd.map(lambda user: Row(
        user_id = user[1],
        user_name = user[0][0],
    ))
    users_df = spark.createDataFrame(users_rdd)
    return users_df

def prepare_listen_data(table):
    listens_df = spark.sql("""
            SELECT listened_at
                 , track_name
                 , recording_msid
                 , user_name
             From %s
        """ % (table))
    return listens_df

def prepare_recording_data(table):
    recordings_q = spark.sql("""
            SELECT track_name
                 , recording_msid
             From %s
        """ % (table))
    recordings_rdd = recordings_q.rdd.zipWithIndex()
    recordings_rdd = recordings_rdd.map(lambda recording: Row(
        recording_id = recording[1],
        recording_msid = recording[0][1],
        track_name = recording[0][0],
    ))
    recordings_df = spark.createDataFrame(recordings_rdd)
    return recordings_df

def get_playcounts_data(listens_df, users_df, recordings_df):

    listens_df.createOrReplaceTempView('listen')
    users_df.createOrReplaceTempView('user')
    recordings_df.createOrReplaceTempView('recording')
    playcounts_df = spark.sql("""
        SELECT user_id,
               recording_id,
               count(recording_id) as count
          FROM listen
    INNER JOIN user
            ON listen.user_name = user.user_name
    INNER JOIN recording
            ON recording.recording_msid = listen.recording_msid
      GROUP BY user_id, recording_id
      ORDER BY user_id
    """)
    return playcounts_df

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python create_datafreames.py [data_dir]")
        sys.exit(0)
    listenbrainz_spark.init_spark_session('Create_Dataframe')
    date = datetime.utcnow()
    df = None
    for y in range(2005, date.year+1):
        for m in range(1, 13):
            try:
                month = listenbrainz_spark.sql_context.read.parquet('{}/data/listenbrainz/{}/{}.parquet'.format(config.HDFS_CLUSTER_URI, y, m))
                df = df.union(month) if df else month
            except:
                print("No listens for {}/{}".format(m,y))
                continue
    df_directory = sys.argv[1]
    df.printSchema()
    print("Registering Dataframe...")

    table = 'df_to_train_{}'.format(datetime.strftime(date, '%Y_%m_%d'))
    df.createOrReplaceTempView(table)

    print("Preparing user data...")
    users_df = prepare_user_data(table)
    print("Load data dump...")
    listens_df = prepare_listen_data(table)
    print("Prepare recording dump...")
    recordings_df = prepare_recording_data(table)
    print("Get playcounts...")
    playcounts_df = get_playcounts_data(listens_df, users_df, recordings_df)

    # persist all dfs
    print("Persist users...")
    users_df.write.format("parquet").save(os.path.join(df_directory, "user.parquet"))
    print("Persist listens...")
    listens_df.write.format("parquet").save(os.path.join(df_directory, "listen.parquet"))
    print("Persist recordings...")
    recordings_df.write.format("parquet").save(os.path.join(df_directory, "recording.parquet"))
    print("Persist playcounts...")
    playcounts_df.write.format("parquet").save(os.path.join(df_directory, "playcount.parquet"))

    




