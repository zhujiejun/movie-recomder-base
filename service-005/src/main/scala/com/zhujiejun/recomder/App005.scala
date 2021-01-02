package com.zhujiejun.recomder

import com.zhujiejun.recomder.cons.Const._
import com.zhujiejun.recomder.data._
import com.zhujiejun.recomder.util.ConnHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.sql.EsSparkSQL
import redis.clients.jedis.JedisCluster

import scala.collection.JavaConversions._

object App005 {
    def getUserRecentlyRating(num: Int, uid: Int, jedis: JedisCluster): Array[(Int, Double)] = {
        //从redis读取数据,用户评分数据保存在 uid:UID 为key的队列里,value是 MID:SCORE
        jedis.lrange("uid:" + uid, 0, num - 1)
            .map { item => //具体每个评分又是以冒号分隔的两个值
                val attr = item.split(":")
                (attr(0).trim.toInt, attr(1).trim.toDouble)
            }.toArray
    }

    def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map
        [Int, scala.collection.immutable.Map[Int, Double]])(implicit ratings: Array[(Int, Int)]): Array[Int] = {
        //1.从相似度矩阵中拿到所有相似的电影
        val allSimMovies = simMovies(mid).toArray
        //2.查询用户已看过的电影
        val ratingExist = ratings.filter { rating =>
            rating._1 == uid
        }.map { item =>
            item._2.toInt
        }
        //3.把看过的过滤,得到输出列表
        allSimMovies.filter(x => !ratingExist.contains(x._1))
            .sortWith(_._2 > _._2)
            .take(num)
            .map(x => x._1)
    }

    def computeMovieScores(candidateMovies: Array[Int], userRecentlyRatings: Array[(Int, Double)],
                           simMovies: scala.collection.Map[Int, scala.collection.immutable.Map
                               [Int, Double]]): Array[(Int, Double)] = {
        //定义一个ArrayBuffer,用于保存每一个备选电影的基础得分
        val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
        //定义一个HashMap,保存每一个备选电影的增强/减弱因子
        val increMap = scala.collection.mutable.HashMap[Int, Int]()
        val decreMap = scala.collection.mutable.HashMap[Int, Int]()
        for (candidateMovie <- candidateMovies; userRecentlyRating <- userRecentlyRatings) {
            //拿到备选电影和最近评分电影的相似度
            val simScore = getMoviesSimScore(candidateMovie, userRecentlyRating._1, simMovies)
            if (simScore > 0.7) {
                //计算备选电影的基础推荐得分
                scores += ((candidateMovie, simScore * userRecentlyRating._2))
                if (userRecentlyRating._2 > 3) {
                    increMap(candidateMovie) = increMap.getOrDefault(candidateMovie, 0) + 1
                } else {
                    decreMap(candidateMovie) = decreMap.getOrDefault(candidateMovie, 0) + 1
                }
            }
        }
        //根据备选电影的mid做groupby,根据公式去求最后的推荐评分
        scores.groupBy(_._1).map { //groupBy之后得到的数据Map(mid -> ArrayBuffer[(mid, score)])
            case (mid, scoreList) =>
                (mid, scoreList.map(_._2).sum / scoreList.length
                    + log(increMap.getOrDefault(mid, 1))
                    - log(decreMap.getOrDefault(mid, 1)))
        }.toArray.sortWith(_._2 > _._2)
    }

    //获取两个电影之间的相似度
    def getMoviesSimScore(mid1: Int, mid2: Int, simMovies: scala.collection.Map
        [Int, scala.collection.immutable.Map[Int, Double]]): Double = {
        simMovies.get(mid1) match {
            case Some(sims) => sims.get(mid2) match {
                case Some(score) => score
                case None => 0.0
            }
            case None => 0.0
        }
    }

    //求一个数的对数,利用换底公式,底数默认为10
    def log(m: Int): Double = {
        val N = 10
        math.log(m) / math.log(N)
    }

    def main(args: Array[String]): Unit = {
        val sparkConfig: SparkConf = new SparkConf().setMaster(CONFIG("spark.cores")).setAppName(SERVICE_005_NAME)
        sparkConfig.setAll(SPARK_PARAM).setAll(ELASTICS_PARAM)
            .registerKryoClasses(Array(classOf[MovieSearch], classOf[RatingSearch], classOf[TagSearch]))
        val spark = SparkSession.builder().config(sparkConfig).getOrCreate()
        val sparkContext = spark.sparkContext
        val streamingContext = new StreamingContext(sparkContext, Seconds(2))

        import spark.implicits._
        //查询用户已看过的电影
        implicit val ratings: Array[(Int, Int)] = EsSparkSQL.esDF(spark, ORIGINAL_RATING_INDEX).as[Rating].rdd.map { rating =>
            (rating.uid.toInt, rating.mid.toInt)
        }.collect()

        //EsSparkSQL.esDF(spark, MOVIE_CONTENTS_RECS_INDEX)
        val simMovieMatrix = EsSparkSQL.esDF(spark, MOVIE_CONTENTS_RECS_INDEX).as[MovieRecs].rdd.map { movieRecs =>
            (movieRecs.mid.toInt, movieRecs.recs.map(x => (x.mid.toInt, x.score)).toMap) //为了查询相似度方便,转换成map
        }.collectAsMap()

        //加载电影相似度矩阵数据,把它广播出去
        val simMovieMatrixBroadCast = sparkContext.broadcast(simMovieMatrix)
        //通过kafka创建一个DStream
        val kafkaStream = KafkaUtils.createDirectStream[String, String](streamingContext, LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(CONFIG("kafka.to.topic")), KAFKA_PARAM))
        //把原始数据UID|MID|SCORE|TIMESTAMP转换成评分流
        val ratingStream = kafkaStream.map { msg =>
            val attr = msg.value().split("\\|")
            (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
        }
        //继续做流式处理,核心实时算法部分
        ratingStream.foreachRDD { rdds =>
            rdds.foreach {
                case (uid, mid, score, timestamp) =>
                    println("rating data coming! >>>>>>>>>>>>>>>>")
                    //1.从Redis里获取当前用户最近的K次评分,保存成Array[(mid, score)]
                    val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)
                    //2.从相似度矩阵中取出当前电影最相似的N个电影,作为备选列表,Array[mid]
                    val candidateMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)
                    //3.对每个备选电影,计算推荐优先级,得到当前用户的实时推荐列表,Array[(mid, score)]
                    val streamRecs = computeMovieScores(candidateMovies, userRecentlyRatings, simMovieMatrixBroadCast.value)
                    //4.把推荐数据保存到Elastic
                    val recs = streamRecs.map {
                        case (mid, score) => Recommendation(mid, score)
                    }.toSeq
                    List(UserRecs(uid, recs)).toDF().show()
                //List(UserRecs(uid, recs)).toDF().rdd.saveToEs(STREAM_USER_RECS_INDEX)
            }
        }
        //开始接收和处理数据
        streamingContext.start()
        println(">>>>>>>>>>>>>>> streaming started!")
        streamingContext.awaitTermination()
    }
}
