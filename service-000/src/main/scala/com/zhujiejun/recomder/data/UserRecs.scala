package com.zhujiejun.recomder.data

//定义基于预测评分的用户推荐列表
case class UserRecs(uid: BigInt, recs: Seq[Recommendation]) extends Serializable
