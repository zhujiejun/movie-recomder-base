package com.zhujiejun.recomder.util

import com.zhujiejun.recomder.cons.Const._
import org.jblas.DoubleMatrix

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

object SFBUtil {
    //求向量余弦相似度
    def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
        movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
    }

    //时间戳转年月
    def toYearMonth(timestamp: Long): String = {
        val instant = Instant.ofEpochMilli(timestamp)
        val localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault)
        localDateTime.format(DateTimeFormatter.ofPattern(YEAR_MONTH_PATTERN))
    }
}
