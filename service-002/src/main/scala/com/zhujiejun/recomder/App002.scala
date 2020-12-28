package com.zhujiejun.recomder

import com.zhujiejun.recomder.cons.Const._
import com.zhujiejun.recomder.data._
import com.zhujiejun.recomder.util.HBaseUtil
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating => MLRating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

object App002 {
    //求向量余弦相似度
    def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
        movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
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

        import spark.implicits._
        val ratings: List[Rating] = HBaseUtil.getRatingsFromHbase(HBASE_MOVIE_TABLE_NAME, HBASE_RATING_COLUMN_FAMILY)
        val ratingRDD = spark.sparkContext.parallelize(ratings).map { rating =>
            (rating.uid, rating.mid, rating.score) //转化成rdd,并且去掉时间戳
        }.cache()
        //从rating数据中提取所有的uid和mid,并去重
        val userRDD = ratingRDD.map(_._1).distinct()
        val movieRDD = ratingRDD.map(_._2).distinct()
        //训练隐语义模型
        val trainData = ratingRDD.map(x => MLRating(x._1, x._2, x._3))
        val (rank, iterations, lambda) = (200, 5, 0.1)
        val model = ALS.train(trainData, rank, iterations, lambda)
        //基于用户和电影的隐特征，计算预测评分，得到用户的推荐列表
        //计算user和movie的笛卡尔积，得到一个空评分矩阵
        val userMoviesRDD = userRDD.cartesian(movieRDD)
        //调用model的predict方法预测评分
        val preRatings = model.predict(userMoviesRDD)
        val userRecsRDD = preRatings
            .filter {
                _.rating > 0 //过滤出评分大于0的项
            }
            .map { rating =>
                (rating.user, (rating.product, rating.rating))
            }
            .groupByKey
            .map {
                case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION)
                    .map(x => Recommendation(x._1, x._2)))
            }.toDF().rdd
        userRecsRDD.foreach { item =>
            val rowKey = RandomStringUtils.randomAlphanumeric(18)
            USER_RECS_fIELD_NAMES.foreach { k =>
                val v = k match {
                    case "uid" => item.get(0).toString
                    case "recs" => item.get(1).toString
                    case _ => ""
                }
                HBaseUtil.addRowData(OFFLINE_MOVIE_TABLE_NAME, rowKey, USER_RECS_COLUMN_FAMILY, k, v)
            }
        }


        //基于电影隐特征，计算相似度矩阵，得到电影的相似度列表
        val movieFeaturesRDD = model.productFeatures.map {
            case (mid, features) => (mid, new DoubleMatrix(features))
        }
        //对所有电影两两计算它们的相似度，先做笛卡尔积
        val movieRecsRDD = movieFeaturesRDD.cartesian(movieFeaturesRDD)
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
            }.toDF().rdd
        movieRecsRDD.foreach { item =>
            val rowKey = RandomStringUtils.randomAlphanumeric(18)
            MOVIE_RECS_fIELD_NAMES.foreach { k =>
                val v = k match {
                    case "mid" => item.get(0).toString
                    case "recs" => item.get(1).toString
                    case _ => ""
                }
                HBaseUtil.addRowData(OFFLINE_MOVIE_TABLE_NAME, rowKey, MOVIE_RECS_COLUMN_FAMILY, k, v)
            }
        }

        spark.close()
    }
}
