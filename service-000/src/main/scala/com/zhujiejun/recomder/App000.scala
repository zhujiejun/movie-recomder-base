package com.zhujiejun.recomder

import com.zhujiejun.recomder.cons.Const._
import com.zhujiejun.recomder.data.{Movie, MovieSearch, RatingSearch, TagSearch}
import com.zhujiejun.recomder.util.HBaseUtil
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object App000 {
    def storeDataInHabse(rowKey: String, columnFamily: String, column: String, value: String): Unit = {
        println(s"----------the rowKey: $rowKey columnFamily: $columnFamily column: $column value: $value----------")
        if (!HBaseUtil.isTableExist(HBASE_MOVIE_TABLE_NAME)) {
            HBaseUtil.createTable(HBASE_MOVIE_TABLE_NAME, columnFamily)
        }
        HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, columnFamily, column, value)
    }

    def main(args: Array[String]): Unit = {
        /*Array(classOf[MovieSearch], classOf[RatingSearch]) foreach println
        return*/

        val sparkConf = new SparkConf().setMaster(CONFIG("spark.cores")).setAppName(SERVICE_001_NAME)
        sparkConf
            .set("spark.submit.deployMode", "cluster")
            .set("spark.jars", DRIVER_PATH)
            .set("spark.driver.cores", "6")
            .set("spark.driver.memory", "512m")
            .set("spark.executor.cores", "6")
            .set("spark.executor.memory", "512m")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(classOf[MovieSearch], classOf[RatingSearch], classOf[TagSearch]))
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        /*var classNames: String = ""
        sparkConf.get("spark.kryo.classesToRegister").foreach { char =>
            classNames = classNames + char
        }
        println(s"----------the class names is: $classNames----------")
        spark.close()
        return*/

        //import spark.implicits._
        val movieSearch = new MovieSearch()
        val movie = spark.sparkContext.textFile(MOVIE_DATA_PATH)
        val movieRDD = movie.map(item => {
            val attr = item.split("\\^")
            Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim,
                attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
        })
        movieSearch.getFilterMovieRDD(movieRDD).foreach(movie => {
            println(s"--------------------the movie is ${movie.toString}--------------------")
            val rowKey = RandomStringUtils.randomAlphanumeric(18)
            MOVIE_fIELD_NAMES.foreach { k =>
                val v = k match {
                    case "mid" => movie.mid
                    case "name" => movie.name
                    case "descri" => movie.descri
                    case "timelong" => movie.timelong
                    case "issue" => movie.issue
                    case "shoot" => movie.shoot
                    case "language" => movie.language
                    case "genres" => movie.genres
                    case "actors" => movie.actors
                    case "directors" => movie.directors
                }
                storeDataInHabse(rowKey, HBASE_MOVIE_COLUMN_FAMILY, k, v.toString)
            }
        })

        /*val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
        val ratingDF = ratingRDD.map(item => {
            val attr = item.split(",")
            Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
        }).toDF()

        val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
        val tagDF = tagRDD.map(item => {
            val attr = item.split(",")
            Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
        }).toDF()*/

        spark.stop()
    }
}
