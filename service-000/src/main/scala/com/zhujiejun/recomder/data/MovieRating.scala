package com.zhujiejun.recomder.data

//基于评分数据的LFM，只需要rating数据
@deprecated
case class MovieRating(uid: BigInt, mid: BigInt, score: Double, timestamp: BigInt)
