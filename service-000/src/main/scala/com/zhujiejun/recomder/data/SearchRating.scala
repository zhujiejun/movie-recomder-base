package com.zhujiejun.recomder.data

import org.apache.spark.rdd.RDD

case class SearchRating() {
    def getFilterRatingRDD(rdd: RDD[Rating]): RDD[Rating] = {
        rdd.distinct()
    }
}
