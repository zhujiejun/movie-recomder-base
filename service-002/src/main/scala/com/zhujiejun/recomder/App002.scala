package com.zhujiejun.recomder

import com.zhujiejun.recomder.cons.Const._
import com.zhujiejun.recomder.data._
import com.zhujiejun.recomder.util.SFBUtil._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating => MLRating}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sparkRDDFunctions
import org.elasticsearch.spark.sql.EsSparkSQL
import org.jblas.DoubleMatrix

object App002 {
    def main(args: Array[String]): Unit = {
        val sparkConfig: SparkConf = new SparkConf().setMaster(CONFIG("spark.cores")).setAppName(SERVICE_002_NAME)
        sparkConfig
            .setAll(ELASTICS_PARAM)
            .set("spark.driver.cores", "6")
            .set("spark.driver.memory", "512m")
            .set("spark.executor.cores", "6")
            .set("spark.executor.memory", "2g")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(classOf[MovieSearch], classOf[RatingSearch], classOf[TagSearch]))
        val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

        import spark.implicits._
        val ratingRDD = EsSparkSQL.esDF(spark, ORIGINAL_RATING_INDEX).as[Rating].rdd.map { rating =>
            (rating.uid.toInt, rating.mid.toInt, rating.score) //转化成rdd,并且去掉时间戳
        } /*.cache()*/

        //训练隐语义模型
        val trainData = ratingRDD.map(x => MLRating(x._1, x._2, x._3))
        //val (rank, iterations, lambda) = (200, 5, 0.1)
        val (rank, iterations, lambda) = (300, 5, 0.9074302725757746)
        val model = ALS.train(trainData, rank, iterations, lambda)
        //基于电影隐特征,计算相似度矩阵,得到电影的相似度列表
        val movieFeaturesRDD = model.productFeatures.map {
            case (mid, features) => (mid, new DoubleMatrix(features))
        }
        //对所有电影两两计算它们的相似度,先做笛卡尔积
        val movieFeaturesMatrixRDD = movieFeaturesRDD.cartesian(movieFeaturesRDD)
            .filter {
                case (a, b) => a._1 != b._1 //把自己跟自己的配对过滤掉
            }
            .map {
                case (a, b) => val simScore = consinSim(a._2, b._2)
                    (a._1, (b._1, simScore))
            }
            .filter {
                _._2._2 > 0.6 //过滤出相似度大于0.6的
            }
            .groupByKey
            .map {
                case (mid, items) => MovieRecs(mid, items.toList.sortWith(_._2 > _._2)
                    .map(x => Recommendation(x._1, x._2)))
            }
        movieFeaturesMatrixRDD.saveToEs(MOVIE_FEATURES_RECS_INDEX)

        spark.close()
    }
}
