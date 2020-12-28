package com.zhujiejun.recomder.cons

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

    val USER_MAX_RECOMMENDATION = 20
    val HBASE_ZOOKEEPER_QUORUM = "node101"
    val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"

    val CONFIG = Map(
        "spark.cores" -> "local[*]"
    )

    //原始数据表的名称
    val MOVIE_fIELD_NAMES = List("mid", "name", "descri", "timelong", "issue", "shoot", "language", "genres", "actors", "directors")
    val RATING_fIELD_NAMES = List("uid", "mid", "score", "timestamp")
    val TAG_fIELD_NAMES = List("uid", "mid", "tag", "timestamp")

    val HBASE_MOVIE_TABLE_NAME = "sfb_original"
    val HBASE_MOVIE_COLUMN_FAMILY = "sfb_original_movie"
    val HBASE_RATING_COLUMN_FAMILY = "sfb_original_rating"
    val HBASE_TAG_COLUMN_FAMILY = "sfb_original_tag"

    //统计的表的名称
    val RATE_MORE_MOVIES_fIELD_NAMES = List("mid", "count")
    val RATE_MORE_RECENTLY_MOVIES_fIELD_NAMES = List("mid", "count", "yearmonth")
    val AVERAGE_MOVIES_fIELD_NAMES = List("mid", "avg")
    val GENRES_TOP_MOVIES_fIELD_NAMES = List("genres", "recs")

    val STATIC_MOVIE_TABLE_NAME = "sfb_static"
    val RATE_MORE_MOVIES_COLUMN_FAMILY = "rate-more-movies"
    val RATE_MORE_RECENTLY_MOVIES_COLUMN_FAMILY = "rate-more-recently-movies"
    val AVERAGE_MOVIES_COLUMN_FAMILY = "average-movies"
    val GENRES_TOP_MOVIES_COLUMN_FAMILY = "genres-top-movies"

    //离线的表的名称
    val USER_RECS_fIELD_NAMES = List("uid", "recs")
    val MOVIE_RECS_fIELD_NAMES = List("mid", "recs")

    val OFFLINE_MOVIE_TABLE_NAME = "sfb_offline"
    val USER_RECS_COLUMN_FAMILY = "user-recs"
    val MOVIE_RECS_COLUMN_FAMILY = "movie-recs"

    //----------the result is (300,0.1,0.9074302725757746)----------
}
