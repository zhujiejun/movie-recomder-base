package com.zhujiejun.recomder

import com.zhujiejun.recomder.cons.Const.{CONFIG, SERVICE_003_NAME}
import com.zhujiejun.recomder.data.{MovieSearch, RatingSearch, TagSearch}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object App003 {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster(CONFIG("spark.cores")).setAppName(SERVICE_003_NAME)
        sparkConf
            /*.set("spark.submit.deployMode", "cluster")
            .set("spark.jars", DRIVER_PATH)*/
            .set("spark.driver.cores", "6")
            .set("spark.driver.memory", "512m")
            .set("spark.executor.cores", "6")
            .set("spark.executor.memory", "1g")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(classOf[MovieSearch], classOf[RatingSearch], classOf[TagSearch]))
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        //streaming context
        val sparkContext = spark.sparkContext
        val streamingContext = new StreamingContext(sparkContext, Seconds(2))


        spark.close()
    }
}
