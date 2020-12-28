package com.zhujiejun.recomder.data

import org.apache.spark.rdd.RDD

case class SearchMovie() {
    def getFilterMovieRDD(rdd: RDD[Movie]): RDD[Movie] = {
        rdd.distinct()
    }
}
