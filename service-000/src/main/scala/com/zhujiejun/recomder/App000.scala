package com.zhujiejun.recomder

import com.zhujiejun.recomder.cons.Const._
import com.zhujiejun.recomder.data._
import com.zhujiejun.recomder.util.HBaseUtil
import com.zhujiejun.recomder.util.HBaseUtil._
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object App000 {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster(CONFIG("spark.cores")).setAppName(SERVICE_000_NAME)
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

        import spark.implicits._
        checkTableExistInHabse(HBASE_MOVIE_TABLE_NAME)
        val movie = spark.sparkContext.textFile(MOVIE_DATA_PATH)
        movie.map(item => {
            val attr = item.split("\\^")
            Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim,
                attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
        }).toDS().foreach { item =>
            val rowKey = RandomStringUtils.randomAlphanumeric(18)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_MOVIE_COLUMN_FAMILY, "mid", item.mid.toString)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_MOVIE_COLUMN_FAMILY, "name", item.name)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_MOVIE_COLUMN_FAMILY, "descri", item.descri)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_MOVIE_COLUMN_FAMILY, "timelong", item.timelong)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_MOVIE_COLUMN_FAMILY, "issue", item.issue)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_MOVIE_COLUMN_FAMILY, "shoot", item.shoot)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_MOVIE_COLUMN_FAMILY, "language", item.language)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_MOVIE_COLUMN_FAMILY, "genres", item.genres)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_MOVIE_COLUMN_FAMILY, "actors", item.actors)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_MOVIE_COLUMN_FAMILY, "directors", item.directors)
        }

        val rating = spark.sparkContext.textFile(RATING_DATA_PATH)
        rating.map(item => {
            val attr = item.split(",")
            Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
        }).toDS().foreach { item =>
            val rowKey = RandomStringUtils.randomAlphanumeric(18)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_RATING_COLUMN_FAMILY, "uid", item.uid.toString)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_RATING_COLUMN_FAMILY, "mid", item.mid.toString)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_RATING_COLUMN_FAMILY, "score", item.score.toString)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_RATING_COLUMN_FAMILY, "timestamp", item.timestamp.toString)
        }

        val tag = spark.sparkContext.textFile(TAG_DATA_PATH)
        tag.map(item => {
            val attr = item.split(",")
            Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
        }).toDS().foreach { item =>
            val rowKey = RandomStringUtils.randomAlphanumeric(18)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_TAG_COLUMN_FAMILY, "tag", item.tag)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_TAG_COLUMN_FAMILY, "uid", item.uid.toString)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_TAG_COLUMN_FAMILY, "mid", item.mid.toString)
            HBaseUtil.addRowData(HBASE_MOVIE_TABLE_NAME, rowKey, HBASE_TAG_COLUMN_FAMILY, "timestamp", item.timestamp.toString)
        }

        spark.stop()
    }
}
