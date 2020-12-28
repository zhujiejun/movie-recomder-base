package com.zhujiejun.recomder.data

import org.apache.spark.rdd.RDD

class MovieSearch() {
    def getFilterMovieRDD(rdd: RDD[Movie]): RDD[Movie] = {
        rdd.distinct()
    }
}
