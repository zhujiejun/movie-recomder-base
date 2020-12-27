package com.zhujiejun.recomder.data

/**
 * Tag数据集
 *
 * 15,
 * 1955,
 * dentist,
 * 1193435061
 */
@SerialVersionUID(1003L)
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int) extends Serializable
