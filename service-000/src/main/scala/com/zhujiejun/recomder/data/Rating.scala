package com.zhujiejun.recomder.data

import org.apache.spark.rdd.RDD

/**
 * Rating数据集
 *
 * 1,
 * 31,
 * 2.5,
 * 1260759144
 */
@SerialVersionUID(1002L)
case class Rating(uid: BigInt, mid: BigInt, score: Double, timestamp: BigInt) extends Serializable
