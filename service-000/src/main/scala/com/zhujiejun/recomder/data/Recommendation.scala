package com.zhujiejun.recomder.data

//定义一个基准推荐对象
@SerialVersionUID(1008L)
case class Recommendation(mid: BigInt, score: Double) extends Serializable
