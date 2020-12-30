package com.zhujiejun.recomder

import com.zhujiejun.recomder.cons.Const._
import com.zhujiejun.recomder.data._
import com.zhujiejun.recomder.util.HBaseUtil
import com.zhujiejun.recomder.util.HBaseUtil.checkTableExistInHabse
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Date

object App001 {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster(CONFIG("spark.cores")).setAppName(SERVICE_001_NAME)
        sparkConf
            .set("spark.driver.cores", "6")
            .set("spark.driver.memory", "512m")
            .set("spark.executor.cores", "6")
            .set("spark.executor.memory", "2g")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(classOf[MovieSearch], classOf[RatingSearch], classOf[TagSearch]))
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._
        checkTableExistInHabse(STATIC_MOVIE_TABLE_NAME)
        val movies: List[Movie] = HBaseUtil.getMoviesFromHbase(ORIGINAL_MOVIE_TABLE_NAME, ORIGINAL_MOVIE_COLUMN_FAMILY)
        val movieDF = spark.sparkContext.parallelize(movies).toDF()

        val ratings: List[Rating] = HBaseUtil.getRatingsFromHbase(ORIGINAL_MOVIE_TABLE_NAME, ORIGINAL_RATING_COLUMN_FAMILY)
        val ratingDF = spark.sparkContext.parallelize(ratings).toDF()

        //创建名为ratings_tmp的临时表
        ratingDF.createOrReplaceTempView("ratings_tmp")

        //不同的统计推荐结果
        //1.历史热门统计,历史评分数据最多,mid,count
        val rateMoreMoviesDS = spark.sql("select mid, count(mid) count from ratings_tmp group by mid").as[RateMoreMovie]
        rateMoreMoviesDS.foreach { item =>
            val rowKey = RandomStringUtils.randomAlphanumeric(18)
            val mid = item.mid.toString
            val count = item.count.toString
            HBaseUtil.addRowData(STATIC_MOVIE_TABLE_NAME, rowKey, RATE_MORE_MOVIES_COLUMN_FAMILY, "mid", mid)
            HBaseUtil.addRowData(STATIC_MOVIE_TABLE_NAME, rowKey, RATE_MORE_MOVIES_COLUMN_FAMILY, "count", count)
        }

        //2.近期热门统计,按照"yyyyMM"格式选取最近的评分数据,统计评分个数,mid,count,yearmonth
        //注册udf,把时间戳转换成年月格式
        spark.udf.register("changeDate", (x: Int) => FORMATTOR.format(new Date(x * 1000L)).toLong)
        //对原始数据做预处理,去掉uid
        val ratingOfYearMonthDF = spark.sql("select mid, score, changeDate(timestamp) yearmonth from ratings_tmp")
        ratingOfYearMonthDF.createOrReplaceTempView("rating_of_Month")
        //从rating_of_Month中查找电影在各个月份的评分,mid,count,yearmonth
        val rateMoreRecentlyMoviesDS = spark.sql("select mid, count(mid) count, yearmonth from rating_of_Month " +
            "group by yearmonth, mid order by yearmonth desc, count desc").as[RateMoreRecentlyMovie]
        rateMoreRecentlyMoviesDS.foreach { item =>
            val rowKey = RandomStringUtils.randomAlphanumeric(18)
            val mid = item.mid.toString
            val count = item.count.toString
            val yearmonth = item.yearmonth.toString
            HBaseUtil.addRowData(STATIC_MOVIE_TABLE_NAME, rowKey, RATE_MORE_RECENTLY_MOVIES_COLUMN_FAMILY, "mid", mid)
            HBaseUtil.addRowData(STATIC_MOVIE_TABLE_NAME, rowKey, RATE_MORE_RECENTLY_MOVIES_COLUMN_FAMILY, "count", count)
            HBaseUtil.addRowData(STATIC_MOVIE_TABLE_NAME, rowKey, RATE_MORE_RECENTLY_MOVIES_COLUMN_FAMILY, "yearmonth", yearmonth)
        }

        //3.优质电影统计,统计电影的平均评分,mid,avg
        val averageMoviesDS = spark.sql("select mid, avg(score) avg from ratings_tmp group by mid").as[AverageMovie]
        averageMoviesDS.foreach { item =>
            val rowKey = RandomStringUtils.randomAlphanumeric(18)
            val mid = item.mid.toString
            val avg = item.avg.toString
            HBaseUtil.addRowData(STATIC_MOVIE_TABLE_NAME, rowKey, AVERAGE_MOVIES_COLUMN_FAMILY, "mid", mid)
            HBaseUtil.addRowData(STATIC_MOVIE_TABLE_NAME, rowKey, AVERAGE_MOVIES_COLUMN_FAMILY, "avg", avg)
        }

        //4.各类别电影Top统计,mid,avg
        //定义所有类别
        val genres = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary",
            "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery",
            "Romance", "Science", "Tv", "Thriller", "War", "Western")
        //把平均评分加入movie表里,加一列,inner join,mid,name,descri,timelong,issue,shoot,language,genres,actors,directors,avg
        val movieWithScoreDF = movieDF.join(averageMoviesDS.toDF(), "mid")
        //为做笛卡尔积,把genres转成rdd
        val genresRDD = spark.sparkContext.makeRDD(genres)
        //计算类别top10,首先对类别和电影做笛卡尔积
        val genresTopMoviesDS = genresRDD.cartesian(movieWithScoreDF.rdd)
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
            }.toDS()
        genresTopMoviesDS.foreach { item =>
            val rowKey = item.genres
            item.recs.foreach { recomder =>
                val mid = recomder.mid.toString
                val score = recomder.score.toString
                HBaseUtil.addRowData(STATIC_MOVIE_TABLE_NAME, rowKey, GENRES_TOP_MOVIES_COLUMN_FAMILY, mid, score)
            }
        }

        spark.close()
    }

}
