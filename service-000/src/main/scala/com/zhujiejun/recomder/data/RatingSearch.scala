package com.zhujiejun.recomder.data

import org.apache.spark.rdd.RDD

class RatingSearch() {
    def getFilterRatingRDD(rdd: RDD[Rating]): RDD[Rating] = {
        rdd.distinct()
    }
}
