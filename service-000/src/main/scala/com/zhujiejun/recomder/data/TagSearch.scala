package com.zhujiejun.recomder.data

import org.apache.spark.rdd.RDD

case class TagSearch() {
    def getFilterRatingRDD(rdd: RDD[Tag]): RDD[Tag] = {
        rdd.distinct()
    }
}
