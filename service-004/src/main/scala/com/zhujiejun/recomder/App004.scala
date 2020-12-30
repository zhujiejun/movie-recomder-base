package com.zhujiejun.recomder

import com.zhujiejun.recomder.cons.Const.CONFIG
import com.zhujiejun.recomder.util.LogProcessorSupplier
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.util.Properties

object App004 {
    def main(args: Array[String]): Unit = {
        //定义kafka streaming的配置
        val settings = new Properties()
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, CONFIG("application.id.config"))
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CONFIG("kafka.brokers"))
        //settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CONFIG("zookeepers"))

        //创建 kafka stream 配置对象
        //val config: StreamsConfig = new StreamsConfig(settings)
        //创建一个拓扑建构器
        val topology: Topology = new Topology()
        //定义流处理的拓扑结构
        topology.addSource("SOURCE", CONFIG("kafka.from.topic"))
            .addProcessor("PROCESSOR", LogProcessorSupplier(), "SOURCE")
            .addSink("SINK", CONFIG("kafka.to.topic"), "PROCESSOR")
        val streams = new KafkaStreams(topology, settings)
        streams.start()
        println("Kafka stream started!>>>>>>>>>>>")
    }
}
