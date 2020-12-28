package com.zhujiejun.recomder.cons

object Const {
    val SERVICE_001_NAME = "DataLoader"
    val SERVICE_002_NAME = "xxxx"

    val DRIVER_PATH = "/home/cat/service-000/lib/service-000.jar"
    val MOVIE_DATA_PATH = "/home/cat/Downloads/common/movies.csv"
    val RATING_DATA_PATH = "/home/cat/Downloads/common/ratings.csv"
    val TAG_DATA_PATH = "/home/cat/Downloads/common/tags.csv"

    val HBASE_ZOOKEEPER_QUORUM = "node101"
    val hbase_zookeeper_property_clientport = "2181"

    val HBASE_MOVIE_TABLE_NAME = "sfb_original"
    val HBASE_MOVIE_COLUMN_FAMILY = "sfb_original_movie"
    val HBASE_RATING_COLUMN_FAMILY = "sfb_original_rating"
    val HBASE_TAG_COLUMN_FAMILY = "sfb_original_tag"

    val CONFIG = Map(
        "spark.cores" -> "local[*]"
    )

    val MOVIE_fIELD_NAMES = List("mid", "name", "descri", "timelong",
        "issue", "shoot", "language", "genres", "actors", "directors")

    val RATING_fIELD_NAMES = List("uid", "mid", "score", "timestamp")

    val TAG_fIELD_NAMES = List("uid", "mid", "tag", "timestamp")
}
