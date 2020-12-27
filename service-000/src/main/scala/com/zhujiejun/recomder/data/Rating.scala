package com.zhujiejun.recomder.data

/**
* Rating数据集
*
* 1,
* 31,
* 2.5,
* 1260759144
*/
@SerialVersionUID(1002L)
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int) extends Serializable
