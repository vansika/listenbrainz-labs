import itertools
import os
import sys
import json
import tempfile
import time
import listenbrainz_spark

from collections import namedtuple
from math import sqrt
from operator import add
from pyspark.mllib.recommendation import ALS, Rating
from pyspark.sql import Row
from datetime import datetime
from listenbrainz_spark import config

Model = namedtuple('Model', 'model error rank lmbda iteration')


def parse_playcount(row):
    return Rating(row['user_id'], row['recording_id'], row['count'])


def compute_rmse(model, data, n):
    """ Compute RMSE (Root Mean Squared Error).
    """
    predictions = model.predictAll(data.map(lambda x: (x.user, x.product)))
    predictions.collect()
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
    t0 = time.time()
    for rank, lmbda, iteration in itertools.product(ranks, lambdas, iterations):
        print('Training model with rank = %.2f, lambda = %.2f, iterations = %d...' % (rank, lmbda, iteration))
        t1 = time.time()
        model = ALS.trainImplicit(training_data, rank, iterations=iteration, lambda_=lmbda, alpha=alpha)
        validation_rmse = compute_rmse(model, validation_data, num_validation)
        print("    RMSE (validation) = %f for the model trained with " % validation_rmse + \
              "rank = %d, lambda = %.1f, and numIter = %d." % (rank, lmbda, iteration))
        print("Model trained in: ", time.time() - t1)
        if best_model is None or validation_rmse < best_model.error:
            best_model = Model(model=model, error=validation_rmse, rank=rank, lmbda=lmbda, iteration=iteration)

    print('Best model has error = %.2f, rank = %.2f, lambda = %.2f, iteration=%d' %
            (best_model.error, best_model.rank, best_model.lmbda, best_model.iteration))
    print("Best model trained in: ", time.time() - t0)
    return best_model


def main(df):
    t0 = time.time()
    training_data, validation_data, test_data = split_data(df)
    training_data.cache()
    validation_data.cache()
    print("Dataframe split in: ", time.time() - t0)
    num_training = training_data.count()
    num_validation = validation_data.count()
    num_test = test_data.count()
    print('%d, %d, %d' % (training_data.count(), validation_data.count(), test_data.count()))
    model = train(training_data, validation_data, num_validation, [8, 12], [0.1, 10.0], [10, 20])
    date = datetime.utcnow()
    path = os.path.join('/', 'data', 'listenbrainz','listenbrainz-recommendation-model-{}-{}-{}'.format(date.year,date.month,date.day))
    model.model.save(listenbrainz_spark.context, config.HDFS_CLUSTER_URI + path)



    
