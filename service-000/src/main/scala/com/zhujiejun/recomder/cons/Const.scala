package com.zhujiejun.recomder.cons

import org.apache.kafka.common.serialization.StringDeserializer

object Const {
    val SERVICE_000_NAME = "data-loader"
    val SERVICE_001_NAME = "statistics-recommender"
    val SERVICE_002_NAME = "offline-recommender"
    val SERVICE_003_NAME = "kafka-process-stream"
    val SERVICE_004_NAME = "streaming-recommender"
    val SERVICE_005_NAME = "content-recommender"

    val DRIVER_PATH = "/home/cat/service-000/lib/service-000.jar"
    val MOVIE_DATA_PATH = "/home/cat/Downloads/common/movies.csv"
    val RATING_DATA_PATH = "/home/cat/Downloads/common/ratings.csv"
    val TAG_DATA_PATH = "/home/cat/Downloads/common/tags.csv"

    val MAX_USER_RATINGS_NUM = 20
    val MAX_SIM_MOVIES_NUM = 20
    val USER_MAX_RECOMMENDATION = 20
    val HBASE_ZOOKEEPER_QUORUM = "node101"
    val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"

    val CONFIG = Map(
        "spark.cores" -> "local[*]",
        "kafka.from.topic" -> "sfb_recomder_log",
        "kafka.to.topic" -> "sfb_recomder",
        "kafka.brokers" -> "node101:9092",
        "zookeepers" -> "node101:2181",
        "application.id.config" -> "logFilter"
    )

    // 定义kafka连接参数
    val KAFKA_PARAM = Map(
        "group.id" -> "recommender",
        "auto.offset.reset" -> "latest",
        "bootstrap.servers" -> "node101:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer]
    )

    //000原始数据表的名称
    val HBASE_MOVIE_TABLE_NAME = "sfb_original"
    val HBASE_MOVIE_COLUMN_FAMILY = "sfb_original_movie"
    val HBASE_RATING_COLUMN_FAMILY = "sfb_original_rating"
    val HBASE_TAG_COLUMN_FAMILY = "sfb_original_tag"

    //001统计的表的名称
    val STATIC_MOVIE_TABLE_NAME = "sfb_static"
    val RATE_MORE_MOVIES_COLUMN_FAMILY = "rate-more-movies"
    val RATE_MORE_RECENTLY_MOVIES_COLUMN_FAMILY = "rate-more-recently-movies"
    val AVERAGE_MOVIES_COLUMN_FAMILY = "average-movies"
    val GENRES_TOP_MOVIES_COLUMN_FAMILY = "genres-top-movies"

    //002离线的表的名称
    val OFFLINE_MOVIE_TABLE_NAME = "sfb_offline"
    val USER_RECS_COLUMN_FAMILY = "user-recs"
    val MOVIE_RECS_COLUMN_FAMILY = "movie-recs"

    //003实时的表的名称
    val STREAM_MOVIE_TABLE_NAME = "sfb_stream"
    val STREAM_RECS_COLUMN_FAMILY = "stream-recs"
}
