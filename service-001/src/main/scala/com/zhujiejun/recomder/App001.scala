package com.zhujiejun.recomder

import com.zhujiejun.recomder.cons.Const._
import com.zhujiejun.recomder.data._
import com.zhujiejun.recomder.util.HBaseUtil
import com.zhujiejun.recomder.util.HBaseUtil.checkTableExistInHabse
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Date

object App001 {
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
        checkTableExistInHabse(STATIC_MOVIE_TABLE_NAME)
        val movies: List[Movie] = HBaseUtil.getMoviesFromHbase(HBASE_MOVIE_TABLE_NAME, HBASE_MOVIE_COLUMN_FAMILY)
        val movieDF = spark.sparkContext.parallelize(movies).toDF()

        val ratings: List[Rating] = HBaseUtil.getRatingsFromHbase(HBASE_MOVIE_TABLE_NAME, HBASE_RATING_COLUMN_FAMILY)
        val ratingDF = spark.sparkContext.parallelize(ratings).toDF()

        //创建名为ratings_tmp的临时表
        ratingDF.createOrReplaceTempView("ratings_tmp")

        //不同的统计推荐结果
        //1.历史热门统计,历史评分数据最多,mid,count
        val rateMoreMoviesRDD = spark.sql("select mid, count(mid) count from ratings_tmp group by mid").rdd
        //把结果写入对应的HBase表中
        rateMoreMoviesRDD.foreach { item =>
            val rowKey = RandomStringUtils.randomAlphanumeric(18)
            RATE_MORE_MOVIES_fIELD_NAMES.foreach { k =>
                val v = k match {
                    case "mid" => item.get(0).toString
                    case "count" => item.get(1).toString
                    case _ => ""
                }
                HBaseUtil.addRowData(STATIC_MOVIE_TABLE_NAME, rowKey, RATE_MORE_MOVIES_COLUMN_FAMILY, k, v)
            }
        }

        //2.近期热门统计，按照"yyyyMM"格式选取最近的评分数据，统计评分个数
        //创建一个日期格式化工具
        val simpleDateFormat = new SimpleDateFormat("yyyyMM")
        //注册udf,把时间戳转换成年月格式
        spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
        //对原始数据做预处理,去掉uid
        val ratingOfYearMonthDF = spark.sql("select mid, score, changeDate(timestamp) yearmonth from ratings_tmp")
        ratingOfYearMonthDF.createOrReplaceTempView("rating_of_Month")
        //从ratingOfMonth中查找电影在各个月份的评分,mid,count,yearmonth
        val rateMoreRecentlyMoviesRDD = spark.sql("select mid, count(mid) count, yearmonth from rating_of_Month " +
            "group by yearmonth, mid order by yearmonth desc, count desc").rdd
        //把结果写入对应的HBase表中
        rateMoreRecentlyMoviesRDD.foreach { item =>
            val rowKey = RandomStringUtils.randomAlphanumeric(18)
            RATE_MORE_RECENTLY_MOVIES_fIELD_NAMES.foreach { k =>
                val v = k match {
                    case "mid" => item.get(0).toString
                    case "count" => item.get(1).toString
                    case "yearmonth" => item.get(2).toString
                    case _ => ""
                }
                HBaseUtil.addRowData(STATIC_MOVIE_TABLE_NAME, rowKey, RATE_MORE_RECENTLY_MOVIES_COLUMN_FAMILY, k, v)
            }
        }

        //3.优质电影统计,统计电影的平均评分,mid,avg
        val averageMoviesDF = spark.sql("select mid, avg(score) avg from ratings_tmp group by mid")
        //把结果写入对应的HBase表中
        averageMoviesDF.rdd.foreach { item =>
            val rowKey = RandomStringUtils.randomAlphanumeric(18)
            AVERAGE_MOVIES_fIELD_NAMES.foreach { k =>
                val v = k match {
                    case "mid" => item.get(0).toString
                    case "avg" => item.get(1).toString
                    case _ => ""
                }
                HBaseUtil.addRowData(STATIC_MOVIE_TABLE_NAME, rowKey, AVERAGE_MOVIES_COLUMN_FAMILY, k, v)
            }
        }

        //4.各类别电影Top统计
        //定义所有类别
        val genres = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary",
            "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery",
            "Romance", "Science", "Tv", "Thriller", "War", "Western")
        //把平均评分加入movie表里,加一列,inner join
        val movieWithScoreDF = movieDF.join(averageMoviesDF.toDF(), "mid")
        //为做笛卡尔积，把genres转成rdd
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
                case (genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1, item._2)))
            }.toDF().rdd
        //把结果写入对应的HBase表中
        genresTopMoviesRDD.foreach { item =>
            val rowKey = RandomStringUtils.randomAlphanumeric(18)
            GENRES_TOP_MOVIES_fIELD_NAMES.foreach { k =>
                val v = k match {
                    case "mid" => item.get(0).toString //todo
                    case "score" => item.get(1).toString
                    case _ => ""
                }
                HBaseUtil.addRowData(STATIC_MOVIE_TABLE_NAME, rowKey, AVERAGE_MOVIES_COLUMN_FAMILY, k, v)
            }
        }

        spark.close()
    }
}
