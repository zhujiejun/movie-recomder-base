package com.zhujiejun.recomder.util

import com.zhujiejun.recomder.cons.Const._
import com.zhujiejun.recomder.data.{MovieSearch, RatingSearch, TagSearch}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.{HostAndPort, JedisCluster}

//定义连接助手对象,序列化
object ConnHelper extends Serializable {
    lazy val jedis = new JedisCluster(new HostAndPort(REDIS_HOST, REDIS_PORT.toInt))

    def main(args: Array[String]): Unit = {
        val sparkConfig = new SparkConf().setMaster(CONFIG("spark.cores")).setAppName(SERVICE_005_NAME)
        sparkConfig.setAll(SPARK_PARAM).setAll(ELASTICS_PARAM)
            .registerKryoClasses(Array(classOf[MovieSearch], classOf[RatingSearch], classOf[TagSearch]))
        val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

        import spark.implicits._
        val fileRDD = spark.sparkContext.textFile("hdfs://node101:9000/sfb/recomder/redis/redis-data.txt")
        val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
        //wordRDD.toDF("line").show()
        wordRDD.toDS().foreach { line =>
            //uid:UID--->MID:SCORE
            val row = line.split("\t")
            val key = "uid:" + row(0)
            val value = row(1) + ":" + row(2)
            jedis.lpush(key, value)
            println(s"---------$key=$value----------")
        }

        spark.close()
    }
}
