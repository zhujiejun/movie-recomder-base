package com.zhujiejun.recomder.util

import redis.clients.jedis.Jedis

//定义连接助手对象,序列化
object ConnHelper extends Serializable {
    lazy val jedis = new Jedis("node101")
}
