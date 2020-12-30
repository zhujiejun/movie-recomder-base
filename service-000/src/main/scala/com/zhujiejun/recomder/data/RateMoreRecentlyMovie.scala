package com.zhujiejun.recomder.data

/**
 * |-- mid: integer (nullable = false)
 * |-- count: long (nullable = false)
 * |-- yearmonth: integer (nullable = false)
 *
 * @param mid
 * @param count
 * @param yearmonth
 */
case class RateMoreRecentlyMovie(mid: Int, count: Long, yearmonth: Int)
