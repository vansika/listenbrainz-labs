import sys
import listenbrainz_spark
import time


def main():
    listenbrainz_spark.init_spark_session(app_name=sys.argv[1])
    user_name = sys.argv[2]
    t0 = time.time()
    print("Loading dataframe...")
    df = listenbrainz_spark.sql_context.read.parquet('hdfs://hadoop-master.spark-network:9000/data/listenbrainz/2018/12.parquet')
    df.printSchema()
    print(df.columns)
    print(df.count())
    print("Dataframe loaded in %.2f s!" % (time.time() - t0))
    df.createOrReplaceTempView('listen')
    print("Running Query...")
    query_t0 = time.time()
    playcounts_df = listenbrainz_spark.session.sql("""
            SELECT user_name,
                   track_name,
                   count(track_name) as count,
              FROM listen
             WHERE user_name = '%s'
          GROUP BY user_name, track_name
          ORDER BY count DESC
             LIMIT 100
        """ % user_name)
    print("Executed query in %.2f s" % (time.time() - query_t0))
    playcounts_df.show()


if __name__ == '__main__':
    main()
