import listenbrainz_spark
import os
import sys
import logging
import time

from listenbrainz_spark import config, train_models, recommend
from pyspark.sql import Row, SparkSession
from datetime import datetime
from listenbrainz_spark.stats import run_query

def prepare_user_data(table):
    t0 = time.time()
    users_df = run_query("""
            SELECT user_name
                  , row_number() over (ORDER BY "user_name") as user_id
             From (SELECT DISTINCT user_name FROM %s)
        """ % (table))
    print("Number of rows in users df: ",users_df.count())
    print("Users data prepared in ", time.time() - t0)
    return users_df

def prepare_listen_data(table):
    t0 = time.time()
    listens_df = run_query("""
            SELECT listened_at
                 , track_name
                 , recording_msid
                 , user_name
             From %s
        """ % (table))
    print("Number of rows in listens df: ",listens_df.count())
    print("Listens data prepared in ", time.time() - t0)
    return listens_df

def prepare_recording_data(table):
    t0 = time.time()
    recordings_df = run_query("""
            SELECT track_name
                 , recording_msid
                 , artist_name
                 , artist_msid
                 , row_number() over (ORDER BY "recording_msid") AS recording_id
             From (SELECT DISTINCT recording_msid, track_name, artist_name, artist_msid FROM %s)
        """ % (table))
    print("Number of rows in recording df: ",recordings_df.count())
    print("Recording data prepared in ", time.time() - t0)
    return recordings_df

def get_playcounts_data(listens_df, users_df, recordings_df):
    t0 = time.time()
    listens_df.createOrReplaceTempView('listen')
    users_df.createOrReplaceTempView('user')
    recordings_df.createOrReplaceTempView('recording')
    playcounts_df = run_query("""
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
    print("Number of rows in playcounts df: ",playcounts_df.count())
    print("Playcount data prepared in ", time.time() - t0)
    return playcounts_df

if __name__ == '__main__':
    listenbrainz_spark.init_spark_session('Create_Dataframe')
    t0 = time.time()
    date = datetime.utcnow()
    df = None
    for y in range(2013, date.year+1):
        for m in range(1, 13):
            try:
                month = listenbrainz_spark.sql_context.read.parquet('{}/data/listenbrainz/{}/{}.parquet'.format(config.HDFS_CLUSTER_URI, y, m))
                df = df.union(month) if df else month
            except Exception as err:
                logging.error("Cannot read files from HDFS: %s / %s. Aborting." % (type(err).__name__, str(err)))
                continue
    df.printSchema()
    print("Registering Dataframe...")
    table = 'df_to_train_{}'.format(datetime.strftime(date, '%Y_%m_%d'))
    df.createOrReplaceTempView(table)
    print("Dataframe registered in: ", time.time() - t0)

    print("Preparing user data...")
    users_df = prepare_user_data(table)
    print("Load data dump...")
    listens_df = prepare_listen_data(table)
    print("Prepare recording dump...")
    recordings_df = prepare_recording_data(table)
    print("Get playcounts...")
    playcounts_df = get_playcounts_data(listens_df, users_df, recordings_df)
    train_models.main(playcounts_df)
    recommend.main(users_df, playcounts_df, recordings_df)
