import itertools
import os
import sys
import json
import tempfile

from collections import namedtuple
from math import sqrt
from operator import add
from pyspark.mllib.recommendation import ALS, Rating
from pyspark.sql import Row, SparkSession
from pyspark import SparkContext
from datetime import datetime
from listenbrainz_spark import hdfs_connection, config

Model = namedtuple('Model', 'model error rank lmbda iteration')


def parse_playcount(row):
    return Rating(row['user_id'], row['recording_id'], row['count'])


def compute_rmse(model, data, n):
    """ Compute RMSE (Root Mean Squared Error).
    """
    predictions = model.predictAll(data.map(lambda x: (x.user, x.product)))
    for x in  predictions.collect():
    	print(x)
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
      .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
      .values()
    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))


def split_data(playcounts_df):
    print(playcounts_df.show())
    training_data, validation_data, test_data = playcounts_df.rdd.map(parse_playcount).randomSplit([4, 1, 1], 45)
    return training_data, validation_data, test_data


def train(training_data, validation_data, num_validation, ranks, lambdas, iterations):
    best_model = None
    alpha = 3.0 # controls baseline confidence growth

    for rank, lmbda, iteration in itertools.product(ranks, lambdas, iterations):
        print('Training model with rank = %.2f, lambda = %.2f, iterations = %d...' % (rank, lmbda, iteration))
        model = ALS.trainImplicit(training_data, rank, iterations=iteration, lambda_=lmbda, alpha=alpha)
        validation_rmse = compute_rmse(model, validation_data, num_validation)
        print("    RMSE (validation) = %f for the model trained with " % validation_rmse + \
              "rank = %d, lambda = %.1f, and numIter = %d." % (rank, lmbda, iteration))
        if best_model is None or validation_rmse < best_model.error:
            best_model = Model(model=model, error=validation_rmse, rank=rank, lmbda=lmbda, iteration=iteration)

    print('Best model has error = %.2f, rank = %.2f, lambda = %.2f, iteration=%d' %
            (best_model.error, best_model.rank, best_model.lmbda, best_model.iteration))
    return best_model


def main(df):
    training_data, validation_data, test_data = split_data(df)
    num_training = training_data.count()
    num_validation = validation_data.count()
    num_test = test_data.count()
    print('%d, %d, %d' % (training_data.count(), validation_data.count(), test_data.count()))
    model = train(training_data, validation_data, num_validation, [8, 12], [0.1, 10.0], [10, 20])
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    with tempfile.TemporaryDirectory() as training_data:
        date = datetime.utcnow()
        model.model.save(sc, os.path.join(training_data, 'listenbrainz-recommendation-model'))
        with open(os.path.join(training_data, 'model-details.json'), 'w') as f:
            print(json.dumps({
                'rank': model.rank,
                'lambda': model.lmbda,
                'iteration': model.iteration,
                'error': model.error,
            }), file=f)
            dest_path = os.path.join('/', 'data', 'listenbrainz', 'training-data-{}-{}-{}'.format(date.year,date.month,date.day))
            hdfs_connection.init_hdfs(config.HDFS_HTTP_URI)
            hdfs_connection.client.upload(hdfs_path=dest_path, local_path=training_data)


    
