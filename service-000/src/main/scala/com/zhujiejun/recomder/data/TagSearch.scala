package com.zhujiejun.recomder.data

import org.apache.spark.rdd.RDD

class TagSearch() {
    def getFilterRatingRDD(rdd: RDD[Tag]): RDD[Tag] = {
        rdd.distinct()
    }
}
