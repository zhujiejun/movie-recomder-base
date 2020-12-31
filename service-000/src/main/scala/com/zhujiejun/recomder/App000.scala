package com.zhujiejun.recomder

import com.zhujiejun.recomder.cons.Const._
import com.zhujiejun.recomder.data._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sparkRDDFunctions

object App000 {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster(CONFIG("spark.cores")).setAppName(SERVICE_000_NAME)
        sparkConf
            .setAll(ELASTICS_PARAM)
            .set("spark.driver.cores", "6")
            .set("spark.driver.memory", "512m")
            .set("spark.executor.cores", "6")
            .set("spark.executor.memory", "2g")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(classOf[MovieSearch], classOf[RatingSearch], classOf[TagSearch]))
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        val movie = spark.sparkContext.textFile(MOVIE_DATA_PATH)
        val movieRDD = movie.map { item => {
            val attr = item.split("\\^")
            Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim,
                attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
        }
        }
        movieRDD.saveToEs(ORIGINAL_MOVIE_COLUMN_FAMILY)

        val rating = spark.sparkContext.textFile(RATING_DATA_PATH)
        val ratingRDD = rating.map { item => {
            val attr = item.split(",")
            Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
        }
        }
        ratingRDD.saveToEs(ORIGINAL_RATING_COLUMN_FAMILY)

        val tag = spark.sparkContext.textFile(TAG_DATA_PATH)
        val tagRDD = tag.map { item => {
            val attr = item.split(",")
            Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
        }
        }
        tagRDD.saveToEs(ORIGINAL_TAG_COLUMN_FAMILY)

        spark.stop()
    }
}
