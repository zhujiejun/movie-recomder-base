package com.zhujiejun.recomder.demo

import com.zhujiejun.recomder.cons.Const._
import com.zhujiejun.recomder.data._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql.EsSparkSQL

object ESDemo {
    def main(args: Array[String]): Unit = {
        val sparkConfig: SparkConf = new SparkConf().setMaster(CONFIG("spark.cores")).setAppName("ESDemo")
        sparkConfig
            .setAll(ELASTICS_PARAM)
            .set("spark.driver.cores", "6")
            .set("spark.driver.memory", "512m")
            .set("spark.executor.cores", "6")
            .set("spark.executor.memory", "2g")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(classOf[MovieSearch], classOf[RatingSearch], classOf[TagSearch]))
        val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

        /*val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
        val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
        sparkContext.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")

        val rdd001: RDD[(String, collection.Map[String, AnyRef])] = sparkContext.esRDD("spark/docs")
        val rdd002: RDD[String] = rdd001.map(_._1)
        rdd002.toDF().show()
        val rdd003: RDD[collection.Map[String, AnyRef]] = rdd001.map(_._2)
        val rdd004: RDD[(String, String, String)] = rdd003.map { m =>
            val one = m.getOrElse("one", "")
            val two = m.getOrElse("two", "")
            val three = m.getOrElse("three", "")
            (one.toString, two.toString, three.toString)
        }
        rdd004.toDF().show()*/

        /*val movie = spark.sparkContext.textFile(MOVIE_DATA_PATH)
        val movieRDD = movie.map { item => {
            val attr = item.split("\\^")
            Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim,
                attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
        }
        }
        movieRDD.saveToEs(ORIGINAL_MOVIE_TABLE_NAME + "/" + ORIGINAL_MOVIE_COLUMN_FAMILY)

        val movieRDD = sparkContext.esRDD(ORIGINAL_MOVIE_TABLE_NAME + "/" + ORIGINAL_MOVIE_COLUMN_FAMILY)
        val movie001RDD = movieRDD.map(_._1)
        val movie001DS = movie001RDD.toDF("id")
        movie001DS.show()

        implicit val encoder: Encoder[Map[String, String]] = newMapEncoder[Map[String, String]]
        val movie002DS = movieRDD.map(_._2).toDF().as[Movie]
        movie002DS.foreach { movie =>
            println(movie.toString)
        }*/
        val movie = spark.sparkContext.textFile(MOVIE_DATA_PATH)
        val movieRDD = movie.map { item => {
            val attr = item.split("\\^")
            Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim,
                attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
        }
        }
        movieRDD.saveToEs(ORIGINAL_MOVIE_COLUMN_FAMILY)

        //implicit val encoder: Encoder[Movie] = Encoders.kryo(classOf[Movie])
        implicit val encoder: Encoder[Movie] = Encoders.bean(classOf[Movie])
        val df = EsSparkSQL.esDF(spark, ORIGINAL_MOVIE_COLUMN_FAMILY)
            //.select("mid", "name", "descri", "timelong", "issue", "shoot","language", "genres", "actors", "directors")*/ //排序
            .orderBy("mid")
        df.show()
        val ds = df.as[Movie]
        ds.show()

        spark.close()
    }
}
