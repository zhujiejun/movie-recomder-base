package com.zhujiejun.recomder

import com.zhujiejun.recomder.cons.Const._
import com.zhujiejun.recomder.data._
import com.zhujiejun.recomder.util.SFBUtil._
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sparkRDDFunctions
import org.elasticsearch.spark.sql.EsSparkSQL
import org.jblas.DoubleMatrix

object App003 {
    def main(args: Array[String]): Unit = {
        val sparkConfig: SparkConf = new SparkConf().setMaster(CONFIG("spark.cores")).setAppName(SERVICE_005_NAME)
        sparkConfig.setAll(SPARK_PARAM).setAll(ELASTICS_PARAM)
            .registerKryoClasses(Array(classOf[MovieSearch], classOf[RatingSearch], classOf[TagSearch]))
        val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

        import spark.implicits._
        val movieTagsDF = EsSparkSQL.esDF(spark, ORIGINAL_MOVIE_INDEX).as[Movie].rdd.map { movie =>
            //提取mid,name,genres三项作为原始内容特征,分词器默认按照空格做分词
            (movie.mid.toInt, movie.name, movie.genres.map(c => if (c == '|') ' ' else c))
        }.toDF("mid", "name", "genres")

        //核心部分： 用TF-IDF从内容信息中提取电影特征向量
        //创建一个分词器,默认按空格分词
        val tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")
        //用分词器对原始数据做转换,生成新的一列words
        val wordsData = tokenizer.transform(movieTagsDF)
        //引入HashingTF工具,可以把一个词语序列转化成对应的词频
        val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)
        val featurizedData = hashingTF.transform(wordsData)
        //引入IDF工具,可以得到idf模型
        val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        //训练idf模型,得到每个词的逆文档频率
        val idfModel = idf.fit(featurizedData)
        //用模型对原数据进行处理,得到文档中每个词的tf-idf,作为新的特征向量
        val rescaledData = idfModel.transform(featurizedData)
        //rescaledData.show(truncate = false)

        //基于电影内容,计算相似度矩阵,得到电影的相似度列表
        val movieContentsRDD = rescaledData.map(row =>
            (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray)
        ).rdd.map { x =>
            (x._1, new DoubleMatrix(x._2))
        }
        //movieFeatures.collect().foreach(println)
        //对所有电影两两计算它们的相似度,先做笛卡尔积
        val movieContentsMatrixRDD = movieContentsRDD.cartesian(movieContentsRDD)
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
        movieContentsMatrixRDD.saveToEs(MOVIE_CONTENTS_RECS_INDEX)

        spark.stop()
    }
}
