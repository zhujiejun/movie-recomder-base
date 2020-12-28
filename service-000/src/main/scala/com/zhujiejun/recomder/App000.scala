package com.zhujiejun.recomder

import com.zhujiejun.recomder.cons.Const._
import com.zhujiejun.recomder.data._
import com.zhujiejun.recomder.util.HBaseUtil
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object App000 {
    private def checkTableExistInHabse(columnFamily: String): Unit = {
        if (!HBaseUtil.isTableExist(HBASE_MOVIE_TABLE_NAME)) {
            println(s"----------the table $HBASE_MOVIE_TABLE_NAME  not existed, create the table----------")
            HBaseUtil.createTable(HBASE_MOVIE_TABLE_NAME, columnFamily)
        }
    }

    private def storeMovieDataInHabse(columnFamily: String)(implicit data: RDD[Movie], save: RDD[Movie] => Unit): Unit = {
        checkTableExistInHabse(columnFamily)
        save(data)
    }

    private def storeRatingDataInHabse(columnFamily: String)(implicit data: RDD[Rating], save: RDD[Rating] => Unit): Unit = {
        checkTableExistInHabse(columnFamily)
        save(data)
    }

    private def storeTagDataInHabse(columnFamily: String)(implicit data: RDD[Tag], save: RDD[Tag] => Unit): Unit = {
        checkTableExistInHabse(columnFamily)
        save(data)
    }

    def main(args: Array[String]): Unit = {
        /*Array(classOf[MovieSearch], classOf[RatingSearch]) foreach println
        return*/

        val sparkConf = new SparkConf().setMaster(CONFIG("spark.cores")).setAppName(SERVICE_001_NAME)
        sparkConf
            /*.set("spark.submit.deployMode", "cluster")
            .set("spark.jars", DRIVER_PATH)*/
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
        //val movieSearch = new MovieSearch()
        val movie = spark.sparkContext.textFile(MOVIE_DATA_PATH)
        implicit val movieRDD: RDD[Movie] = movie.map(item => {
            val attr = item.split("\\^")
            Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim,
                attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
        })

        implicit def movieSaveToHbase(data: RDD[Movie]): Unit = {
            data.foreach(movie => {
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
                    HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_MOVIE_COLUMN_FAMILY, k, v.toString)
                }
            })
        }

        storeMovieDataInHabse(HBASE_MOVIE_COLUMN_FAMILY)

        val rating = spark.sparkContext.textFile(RATING_DATA_PATH)
        implicit val ratingRDD: RDD[Rating] = rating.map(item => {
            val attr = item.split(",")
            Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
        })

        implicit def ratingSaveToHbase(data: RDD[Rating]): Unit = {
            data.foreach(rating => {
                val rowKey = RandomStringUtils.randomAlphanumeric(18)
                RATING_fIELD_NAMES.foreach { k =>
                    val v = k match {
                        case "uid" => rating.uid
                        case "mid" => rating.mid
                        case "score" => rating.score
                        case "timestamp" => rating.timestamp
                    }
                    HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_RATING_COLUMN_FAMILY, k, v.toString)
                }
            })
        }

        storeRatingDataInHabse(HBASE_RATING_COLUMN_FAMILY)

        val tag = spark.sparkContext.textFile(TAG_DATA_PATH)
        implicit val tagRDD: RDD[Tag] = tag.map(item => {
            val attr = item.split(",")
            Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
        })

        implicit def tagSaveToHbase(data: RDD[Tag]): Unit = {
            data.foreach(tag => {
                val rowKey = RandomStringUtils.randomAlphanumeric(18)
                TAG_fIELD_NAMES.foreach { k =>
                    val v = k match {
                        case "uid" => tag.uid
                        case "mid" => tag.mid
                        case "tag" => tag.tag
                        case "timestamp" => tag.timestamp
                    }
                    HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_TAG_COLUMN_FAMILY, k, v.toString)
                }
            })
        }

        storeTagDataInHabse(HBASE_TAG_COLUMN_FAMILY)

        spark.stop()
    }
}
