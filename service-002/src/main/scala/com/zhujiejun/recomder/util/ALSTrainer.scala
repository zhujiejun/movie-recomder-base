package com.zhujiejun.recomder.util

import breeze.numerics.sqrt
import com.zhujiejun.recomder.cons.Const.{CONFIG, HBASE_MOVIE_TABLE_NAME, HBASE_RATING_COLUMN_FAMILY, SERVICE_001_NAME}
import com.zhujiejun.recomder.data.{MovieSearch, Rating, RatingSearch, TagSearch}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating => MLRating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {
    def adjustALSParam(trainData: RDD[MLRating], testData: RDD[MLRating]): Unit = {
        val result = for (rank <- Array(50, 100, 200, 300); lambda <- Array(0.01, 0.1, 1))
            yield {
                val model = ALS.train(trainData, rank, 5, lambda)
                //计算当前参数对应模型的rmse,返回Double
                val rmse = getRMSE(model, testData)
                (rank, lambda, rmse)
            }
        //控制台打印输出最优参数
        println(s"----------the result is ${result.minBy(_._3)}----------")
    }

    def getRMSE(model: MatrixFactorizationModel, data: RDD[MLRating]): Double = {
        // 计算预测评分
        val userProducts = data.map(item => (item.user, item.product))
        val predictRating = model.predict(userProducts)
        // 以uid,mid作为外键,inner join实际观测值和预测值
        val observed = data.map(item => ((item.user, item.product), item.rating))
        val predict = predictRating.map(item => ((item.user, item.product), item.rating))
        // 内连接得到(uid, mid),(actual, predict)
        sqrt(
            observed.join(predict).map {
                case ((uid, mid), (actual, pre)) =>
                    val err = actual - pre
                    err * err
            }.mean()
        )
    }

    def main(args: Array[String]): Unit = {
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
        val ratings: List[Rating] = HBaseUtil.getRatingsFromHbase(HBASE_MOVIE_TABLE_NAME, HBASE_RATING_COLUMN_FAMILY)
        val ratingRDD = spark.sparkContext.parallelize(ratings).map { rating =>
            MLRating(rating.uid, rating.mid, rating.score) //转化成rdd,并且去掉时间戳
        }.cache()
        //随机切分数据集，生成训练集和测试集
        val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
        val trainRDD = splits(0)
        val testRDD = splits(1)
        //模型参数选择，输出最优参数
        adjustALSParam(trainRDD, testRDD)

        spark.close()
    }
}
