package com.zhujiejun.recomder.data

// 定义电影类别top10推荐对象
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])
