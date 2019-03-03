import listenbrainz_spark
from listenbrainz_spark import config
from setup import spark

def prepare_listen_data():
    users_df = spark.sql("""
            SELECT user_name
                 , track_name
                 , recording_msid
                 , count(recording_msid) as cnt
             From listen
        GROUP BY user_name, track_name, recording_msid
        ORDER BY cnt desc
        """)
    return users_df

if __name__ == '__main__':
    listenbrainz_spark.init_spark_session('Create_Datafeame')
    df = None
    for y in range(2005, 2020):
        for m in range(1, 13):
            try:
                month = listenbrainz_spark.sql_context.read.parquet('{}/data/listenbrainz/{}/{}.parquet'.format(config.HDFS_CLUSTER_URI, y, m))
                df = df.union(month) if df else month
            except:
                print("No listens for {}/{}".format(m,y))
                continue
    df.printSchema()
    print("Registering Dataframe...")
    df.registerTempTable('listen')

    print("Preparing user's listen data...")
    users_df = prepare_listen_data() 

    print("Persist listen data")
    users_df.write.format("parquet").save("user.parquet")




