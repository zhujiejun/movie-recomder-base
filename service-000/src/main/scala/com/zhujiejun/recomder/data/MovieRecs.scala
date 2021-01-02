package com.zhujiejun.recomder.data

//定义基于LFM电影特征向量的电影相似度列表
case class MovieRecs(mid: BigInt, recs: Seq[Recommendation]) extends Serializable
