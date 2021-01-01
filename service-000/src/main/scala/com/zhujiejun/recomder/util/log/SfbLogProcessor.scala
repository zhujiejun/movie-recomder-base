package com.zhujiejun.recomder.util.log

import org.apache.kafka.streams.processor.{Processor, ProcessorContext}

class SfbLogProcessor() extends Processor[Array[Byte], Array[Byte]] {
    var context: Option[ProcessorContext] = None

    override def init(context: ProcessorContext): Unit = {
        this.context = Option(context)
    }

    override def process(dummy: Array[Byte], line: Array[Byte]): Unit = {
        //把收集到的日志信息用string表示
        var input = new String(line)
        //根据前缀MOVIE_RATING_PREFIX:从日志信息中提取评分数据
        if (input.contains("MOVIE_RATING_PREFIX:")) {
            println("movie rating data coming!>>>>>>>>>>>" + input)
            input = input.split("MOVIE_RATING_PREFIX:")(1).trim
            context.get.forward("logProcessor".getBytes, input.getBytes)
        }
    }

    override def close(): Unit = {

    }
}
