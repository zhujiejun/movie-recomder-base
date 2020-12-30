package com.zhujiejun.recomder.util

import org.apache.kafka.streams.processor.{Processor, ProcessorSupplier}

case class LogProcessorSupplier() extends ProcessorSupplier[Array[Byte], Array[Byte]] {
    override def get(): Processor[Array[Byte], Array[Byte]] = {
        new LogProcessor()
    }
}
