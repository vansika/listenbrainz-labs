import json
import os
import hdfs
import listenbrainz_spark
import tempfile
import sys
import shutil

from listenbrainz_spark import hdfs_connection
from listenbrainz_spark import config
from pyspark.sql.types import StructField, IntegerType, StringType, StructType


artist_relation_schema = [
	StructField('artist_mbid_0', StringType(), nullable=False),
	StructField('artist_name_0', StringType(), nullable=False),
	StructField('artist_mbid_1', StringType(), nullable=False),
	StructField('artist_name_1', StringType(), nullable=False),
	StructField('count', IntegerType(), nullable=False),
]

artist_relation_schema = StructType(sorted(artist_relation_schema, key=lambda field: field.name))

def copy_to_hdfs(filename):
	tmp_dir = tempfile.mkdtemp()
	hdfs_path = os.path.join('/', 'data', 'listenbrainz', 'similar_artists')
	hdfs_connection.client.delete(hdfs_path, recursive=True)
	temp_hdfs_path = os.path.join(tmp_dir + filename)
	hdfs_connection.client.upload(hdfs_path=temp_hdfs_path, local_path=filename)

	artist_df = listenbrainz_spark.session.read.json(config.HDFS_CLUSTER_URI + temp_hdfs_path, schema=artist_relation_schema)
	artist_df.write.format('parquet').save(config.HDFS_CLUSTER_URI + hdfs_path + '/artist_artist_relations.parquet')
	hdfs_connection.client.delete(temp_hdfs_path)
	hdfs_connection.client.delete(tmp_dir, recursive=True)
	shutil.rmtree(tmp_dir)

def read_similar_artists_parquet():
	artist_df = listenbrainz_spark.sql_context.read.parquet('{}/data/listenbrainz/similar_artists/artist_artist_relations.parquet'.format(config.HDFS_CLUSTER_URI))
	artist_df.printSchema()
	artist_df.show()
if __name__ == '__main__':
	if len(sys.argv) != 2:
		print("Usage: import_similar_artist.py <JSON file to import>")
	filename = sys.argv[1]
	if filename.split(".")[-1] != 'json':
		print("Please input a json file...")
		sys.exit(-1)
	listenbrainz_spark.init_spark_session("Similar Artist")
	hdfs_connection.init_hdfs(config.HDFS_HTTP_URI)
	copy_to_hdfs(filename)
	read_similar_artists_parquet()
	print("DONE!")