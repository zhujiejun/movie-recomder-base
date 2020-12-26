package com.zhujiejun.recomder

import com.zhujiejun.recomder.cons.Const._
import com.zhujiejun.recomder.util.HBaseUtil

object App000 {
    def storeDataInHabse(rowKey: String, columnFamily: String, column: String, value: String): Unit = {
        if (!HBaseUtil.isTableExist(HBASE_MOVIE_TABLE_NAME)) {
            HBaseUtil.createTable(HBASE_MOVIE_TABLE_NAME, columnFamily)
        }
        HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, columnFamily, column, value)
    }

    /*def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster(CONFIG("spark.cores")).setAppName(SERVICE_001_NAME)
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._
        val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
        val movieDF = movieRDD.map(item => {
            val attr = item.split("\\^")
            Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
        }).toDF()
        movieDF.foreach(row => {
            val rowKey = RandomStringUtils.randomAlphanumeric(18)
            for (i <- 0 to 10) {
                storeDataInHabse(rowKey, HBASE_MOVIE_COLUMN_FAMILY, MOVIE_fIELD_MAP(i), row.get(i).toString)
            }
        })

        val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
        val ratingDF = ratingRDD.map(item => {
            val attr = item.split(",")
            Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
        }).toDF()

        val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
        val tagDF = tagRDD.map(item => {
            val attr = item.split(",")
            Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
        }).toDF()

        spark.stop()
    }*/

    def main(args: Array[String]): Unit = {
        0 to 10 foreach println
    }
}
