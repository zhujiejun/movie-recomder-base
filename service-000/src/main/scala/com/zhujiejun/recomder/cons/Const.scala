package com.zhujiejun.recomder.cons

import org.apache.kafka.common.serialization.StringDeserializer

object Const {
    val CONFIG = Map(
        "spark.cores" -> "local[*]",
        "zookeepers" -> "node101:2181",
        "kafka.brokers" -> "node101:9092",
        "kafka.from.topic" -> "sfb_recomder_log",
        "kafka.to.topic" -> "sfb_recomder"
    )

    val SPARK_PARAM: Array[(String, String)] = Map(
        "spark.driver.cores" -> "6",
        "spark.driver.memory" -> "1g ",
        "spark.executor.cores" -> "6",
        "spark.executor.memory" -> "2g",
        "spark.kryoserializer.buffer.max" -> "128m",
        "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer"
    ).toArray

    val KAFKA_PARAM = Map(
        "group.id" -> "recommender",
        "auto.offset.reset" -> "latest",
        "bootstrap.servers" -> "node101:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer]
    )

    val ELASTICS_PARAM: Array[(String, String)] = Map(
        "es.port" -> "9200",
        "es.nodes.wan.only" -> "true",
        "es.index.auto.create" -> "true",
        //"es.mapping.id" -> "zip_record_id",
        "es.nodes" -> "node101,node102,node103",
        "es.read.field.as.array.include" -> "recs"
    ).toArray

    val YEAR_MONTH_PATTERN = "yyyyMM"

    val SERVICE_000_NAME = "data_loader"
    val SERVICE_001_NAME = "statistics_recommender"
    val SERVICE_002_NAME = "offline_recommender"
    val SERVICE_003_NAME = "content_recommender"
    val SERVICE_004_NAME = "kafka_process_stream"
    val SERVICE_005_NAME = "streaming_recommender"


    val REDIS_PORT = "7001"
    val REDIS_HOST = "node101"
    val MAX_USER_RATINGS_NUM = 20
    val MAX_SIM_MOVIES_NUM = 20
    val USER_MAX_RECOMMENDATION = 20
    val HBASE_ZOOKEEPER_QUORUM = "node101"
    val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"

    val DRIVER_PATH = "/home/cat/service_000/lib/service_000.jar"
    val MOVIE_DATA_PATH = "/home/cat/Downloads/common/movies.csv"
    val RATING_DATA_PATH = "/home/cat/Downloads/common/ratings.csv"
    val TAG_DATA_PATH = "/home/cat/Downloads/common/tags.csv"
    //Type names are deprecated and will be removed in a later release.
    //000原始数据索引名称
    val ORIGINAL_MOVIE_INDEX = "original_movie_index"
    val ORIGINAL_RATING_INDEX = "original_rating_index"
    val ORIGINAL_TAG_INDEX = "original_tag_index"
    //001统计索引名称
    val RATE_MORE_MOVIES_INDEX = "rate_more_movies_index"
    val RATE_MORE_RECENTLY_MOVIES_INDEX = "rate_more_recently_movies_index"
    val AVERAGE_MOVIES_INDEX = "average_movies_index"
    val GENRES_TOP_MOVIES_INDEX = "genres_top_movies_index"
    //002|003离线索引名称
    val OFFLINE_USER_RECS_INDEX = "offline_user_recs_index"
    val MOVIE_FEATURES_RECS_INDEX = "movie_features_matrix_index"
    val MOVIE_CONTENTS_RECS_INDEX = "movie_contents_matrix_index"
    //004|005实时索引名称
    val STREAM_USER_RECS_INDEX = "stream_user_recs_index"
}
