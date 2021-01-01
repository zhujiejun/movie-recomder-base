package com.zhujiejun.recomder

import com.zhujiejun.recomder.cons.Const._
import com.zhujiejun.recomder.data._
import com.zhujiejun.recomder.util.SFBUtil._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.elasticsearch.spark.sparkRDDFunctions
import org.elasticsearch.spark.sql.EsSparkSQL

object App001 {
    def main(args: Array[String]): Unit = {
        val sparkConfig: SparkConf = new SparkConf().setMaster(CONFIG("spark.cores")).setAppName(SERVICE_001_NAME)
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
        implicit val movieEncoder: Encoder[Movie] = Encoders.bean(classOf[Movie])
        val movieDF = EsSparkSQL.esDF(spark, ORIGINAL_MOVIE_INDEX).orderBy("mid")
            .as[Movie].toDF()
        movieDF.show()

        implicit val ratingEncoder: Encoder[Rating] = Encoders.bean(classOf[Rating])
        val ratingDF = EsSparkSQL.esDF(spark, ORIGINAL_RATING_INDEX).orderBy("uid", "mid")
            .as[Rating].toDF()
        ratingDF.show()

        //创建名为ratings_tmp的临时表
        ratingDF.createOrReplaceTempView("ratings_tmp")

        //不同的统计推荐结果
        //1.历史热门统计,历史评分数据最多,mid,count
        implicit val encoder1: Encoder[RateMoreMovie] = Encoders.bean(classOf[RateMoreMovie])
        val rateMoreMoviesRDD = spark.sql("select mid, count(mid) count from ratings_tmp group by mid")
            .as[RateMoreMovie].rdd
        rateMoreMoviesRDD.toDF().show()
        rateMoreMoviesRDD.saveToEs(RATE_MORE_MOVIES_INDEX)

        //2.近期热门统计,按照"yyyyMM"格式选取最近的评分数据,统计评分个数,mid,count,yearmonth
        //注册udf,把时间戳转换成年月格式
        spark.udf.register("toYearMonth", (x: Int) => toYearMonth(x * 1000L).toInt)
        //对原始数据做预处理,去掉uid
        val ratingOfYearMonthDF = spark.sql("select mid, score, toYearMonth(timestamp) yearmonth from ratings_tmp")
        ratingOfYearMonthDF.createOrReplaceTempView("rating_of_month")
        //从rating_of_month中查找电影在各个月份的评分,mid,count,yearmonth
        implicit val encoder2: Encoder[RateMoreRecentlyMovie] = Encoders.bean(classOf[RateMoreRecentlyMovie])
        val rateMoreRecentlyMoviesRDD = spark.sql("select mid, count(mid) count, yearmonth from rating_of_month " +
            "group by yearmonth, mid order by yearmonth desc, count desc").as[RateMoreRecentlyMovie].rdd
        rateMoreRecentlyMoviesRDD.saveToEs(RATE_MORE_RECENTLY_MOVIES_INDEX)

        //3.优质电影统计,统计电影的平均评分,mid,avg
        implicit val encoder3: Encoder[AverageMovie] = Encoders.bean(classOf[AverageMovie])
        val averageMoviesRDD = spark.sql("select mid, avg(score) avg from ratings_tmp group by mid")
            .as[AverageMovie].rdd
        averageMoviesRDD.saveToEs(AVERAGE_MOVIES_INDEX)

        //4.各类别电影Top统计,mid,avg
        //定义所有类别
        val genres = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary",
            "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery",
            "Romance", "Science", "Tv", "Thriller", "War", "Western")
        //把平均评分加入movie表里,加一列,inner join,mid,name,descri,timelong,issue,shoot,language,genres,actors,directors,avg
        val movieWithScoreDF = movieDF.join(averageMoviesRDD.toDF(), "mid")
        //为做笛卡尔积,把genres转成rdd
        val genresRDD = spark.sparkContext.makeRDD(genres)
        //计算类别top10,首先对类别和电影做笛卡尔积
        val genresTopMoviesRDD = genresRDD.cartesian(movieWithScoreDF.rdd)
            .filter {
                //条件过滤,找出movie的字段genres值(Action|Adventure|Sci-Fi)包含当前类别genre(Action)的那些
                case (genre, movieRow) => movieRow.getAs[String]("genres").toLowerCase.contains(genre.toLowerCase)
            }
            .map {
                case (genre, movieRow) => (genre, (movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avg")))
            }
            .groupByKey
            .map {
                case (genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(10)
                    .map(item => Recommendation(item._1, item._2)))
            }
        genresTopMoviesRDD.saveToEs(GENRES_TOP_MOVIES_INDEX)

        spark.close()
    }

}
