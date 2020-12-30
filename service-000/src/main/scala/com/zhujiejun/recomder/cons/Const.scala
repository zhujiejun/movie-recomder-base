package com.zhujiejun.recomder.cons

import org.apache.kafka.common.serialization.StringDeserializer

object Const {
    val CONFIG = Map(
        "spark.cores" -> "local[*]",
        "kafka.from.topic" -> "sfb_recomder_log",
        "kafka.to.topic" -> "sfb_recomder",
        "kafka.brokers" -> "node101:9092",
        "zookeepers" -> "node101:2181",
    )

    val KAFKA_PARAM = Map(
        "group.id" -> "recommender",
        "auto.offset.reset" -> "latest",
        "bootstrap.servers" -> "node101:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer]
    )

    val YEAR_MONTH_PATTERN = "yyyyMM"

    val SERVICE_000_NAME = "data_loader"
    val SERVICE_001_NAME = "statistics_recommender"
    val SERVICE_002_NAME = "offline_recommender"
    val SERVICE_003_NAME = "content_recommender"
    val SERVICE_004_NAME = "kafka_process_stream"
    val SERVICE_005_NAME = "streaming_recommender"


    val MAX_USER_RATINGS_NUM = 20
    val MAX_SIM_MOVIES_NUM = 20
    val USER_MAX_RECOMMENDATION = 20
    val HBASE_ZOOKEEPER_QUORUM = "node101"
    val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"

    val DRIVER_PATH = "/home/cat/service_000/lib/service_000.jar"
    val MOVIE_DATA_PATH = "/home/cat/Downloads/common/movies.csv"
    val RATING_DATA_PATH = "/home/cat/Downloads/common/ratings.csv"
    val TAG_DATA_PATH = "/home/cat/Downloads/common/tags.csv"

    //000原始数据表名称
    val ORIGINAL_MOVIE_TABLE_NAME = "sfb_original"
    val ORIGINAL_MOVIE_COLUMN_FAMILY = "original_movie"
    val ORIGINAL_RATING_COLUMN_FAMILY = "original_rating"
    val ORIGINAL_TAG_COLUMN_FAMILY = "original_tag"

    //001统计表名称
    val STATIC_MOVIE_TABLE_NAME = "sfb_static"
    val RATE_MORE_MOVIES_COLUMN_FAMILY = "rate_more_movies"
    val RATE_MORE_RECENTLY_MOVIES_COLUMN_FAMILY = "rate_more_recently_movies"
    val AVERAGE_MOVIES_COLUMN_FAMILY = "average_movies"
    val GENRES_TOP_MOVIES_COLUMN_FAMILY = "genres_top_movies"

    //002|003离线表名称
    val OFFLINE_MOVIE_TABLE_NAME = "sfb_offline"
    val OFFLINE_USER_RECS_COLUMN_FAMILY = "offline_user_recs"
    val MOVIE_FEATURES_RECS_COLUMN_FAMILY = "movie_features_matrix"
    val MOVIE_CONTENTS_RECS_COLUMN_FAMILY = "movie_contents_matrix"

    //004|005实时表名称
    val STREAM_MOVIE_TABLE_NAME = "sfb_stream"
    val STREAM_USER_RECS_COLUMN_FAMILY = "stream_user_recs"
}
