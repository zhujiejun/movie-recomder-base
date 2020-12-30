package com.zhujiejun.recomder.cons

import org.apache.kafka.common.serialization.StringDeserializer

object Const {
    val SERVICE_000_NAME = "data_loader"
    val SERVICE_001_NAME = "statistics_recommender"
    val SERVICE_002_NAME = "offline_recommender"
    val SERVICE_003_NAME = "content_recommender"
    val SERVICE_004_NAME = "kafka_process_stream"
    val SERVICE_005_NAME = "streaming_recommender"

    val DRIVER_PATH = "/home/cat/service_000/lib/service_000.jar"
    val MOVIE_DATA_PATH = "/home/cat/Downloads/common/movies.csv"
    val RATING_DATA_PATH = "/home/cat/Downloads/common/ratings.csv"
    val TAG_DATA_PATH = "/home/cat/Downloads/common/tags.csv"

    val MAX_USER_RATINGS_NUM = 20
    val MAX_SIM_MOVIES_NUM = 20
    val USER_MAX_RECOMMENDATION = 20
    val HBASE_ZOOKEEPER_QUORUM = "node101"
    val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"

    val CONFIG = Map(
        "spark.cores" _ > "local[*]",
        "kafka.from.topic" _ > "sfb_recomder_log",
        "kafka.to.topic" _ > "sfb_recomder",
        "kafka.brokers" _ > "node101:9092",
        "zookeepers" _ > "node101:2181",
    )

    // 定义kafka连接参数
    val KAFKA_PARAM = Map(
        "group.id" _ > "recommender",
        "auto.offset.reset" _ > "latest",
        "bootstrap.servers" _ > "node101:9092",
        "key.deserializer" _ > classOf[StringDeserializer],
        "value.deserializer" _ > classOf[StringDeserializer]
    )

    //000原始数据表的名称
    val ORIGINAL_MOVIE_TABLE_NAME = "sfb_original"
    val ORIGINAL_MOVIE_COLUMN_FAMILY = "original_movie"
    val ORIGINAL_RATING_COLUMN_FAMILY = "original_rating"
    val ORIGINAL_TAG_COLUMN_FAMILY = "original_tag"

    //001统计的表的名称
    val STATIC_MOVIE_TABLE_NAME = "sfb_static"
    val RATE_MORE_MOVIES_COLUMN_FAMILY = "rate_more_movies"
    val RATE_MORE_RECENTLY_MOVIES_COLUMN_FAMILY = "rate_more_recently_movies"
    val AVERAGE_MOVIES_COLUMN_FAMILY = "average_movies"
    val GENRES_TOP_MOVIES_COLUMN_FAMILY = "genres_top_movies"

    //002离线的表的名称
    val OFFLINE_MOVIE_TABLE_NAME = "sfb_offline"
    val USER_RECS_COLUMN_FAMILY = "user_recs"
    val MOVIE_FEATURES_RECS_COLUMN_FAMILY = "movie_features_recs"
    val MOVIE_CONTENTS_RECS_COLUMN_FAMILY = "movie_contents_recs"

    //003实时的表的名称
    val STREAM_MOVIE_TABLE_NAME = "sfb_stream"
    val STREAM_RECS_COLUMN_FAMILY = "stream_recs"
}
