package com.zhujiejun.recomder.cons

object Const {
    val SERVICE_001_NAME = "DataLoader"
    val SERVICE_002_NAME = "xxxx"

    /*val MOVIE_DATA_PATH = "/home/cat/Downloads/common/movies.csv"
    val RATING_DATA_PATH = "/home/cat/Downloads/common/ratings.csv"
    val TAG_DATA_PATH = "/home/cat/Downloads/common/tags.csv"*/

    val MOVIE_DATA_PATH = "/home/cat/movies.csv"
    val RATING_DATA_PATH = "/home/cat/ratings.csv"
    val TAG_DATA_PATH = "/home/cat/tags.csv"

    val HBASE_ZOOKEEPER_QUORUM = "node101"
    val hbase_zookeeper_property_clientport = "2181"

    val HBASE_MOVIE_TABLE_NAME = "sfb_recomder"
    val HBASE_MOVIE_COLUMN_FAMILY = "sfb_movie"
    val HBASE_RATING_COLUMN_FAMILY = "sfb_rating"
    val HBASE_TAG_COLUMN_FAMILY = "sfb_tag"

    val CONFIG = Map(
        //"spark.cores" -> "local[*]"
        "spark.cores" -> "spark://node101:7077"
    )

    val MOVIE_fIELD_MAP = Map(
        0 -> "mid",
        1 -> "name",
        2 -> "descri",
        3 -> "timelong",
        4 -> "issue",
        5 -> "shoot",
        6 -> "language",
        7 -> "genres",
        8 -> "actors",
        9 -> "directors"
    )
}
