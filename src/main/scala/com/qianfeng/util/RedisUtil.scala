package com.qianfeng.util

import java.util.Properties

import com.qianfeng.constant.Constants
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool

/**
 * redis的工具类
 */
object RedisUtil extends Serializable {
  //定义pool和单独的jedis
  private var pool:JedisPool = null
  private var jedis:Jedis = null

  //连接池初始化参数
  private val port = 6379
  private val timeout = 10 * 1000
  private val maxIdle = 10
  private val minIdle = 2
  private val maxTotal = 20

  //初始化参数配置
  private def createConfig = {
    //new对象的时候不能给泛型
    val config: GenericObjectPoolConfig[_] = new GenericObjectPoolConfig
    //添加初始值
    config.setMaxIdle(maxIdle)
    config.setMaxTotal(maxTotal)
    config.setMinIdle(minIdle)
    //返回
    config
  }


  //获取jedis的连接池
  def connectRedisPool(ip: String): JedisPool = {
    if (pool == null) {
      val config: GenericObjectPoolConfig[_] = createConfig
      pool = new JedisPool(config, ip, port, timeout)
    }
    pool
  }

  //获取jedis的连接
  def connectJedis(ip: String, port: Int, auth: String): Jedis = {
    if (jedis == null) {
      jedis = new Jedis(ip, port)
      //设置密码
      jedis.auth(auth)
    }
    jedis
  }


  //测试
  def main(args: Array[String]): Unit = {
    val prop: Properties = PropertiesUtil.readProperty(Constants.REDIS_CONFIG_URL)
    val redis_host: String = prop.getProperty(Constants.REDIS_HOST)
    val redis_port: Int = prop.getProperty(Constants.REDIS_PORT).toInt
    val redis_password: String = prop.getProperty(Constants.REDIS_PASS)
    val jedis: Jedis = connectJedis(redis_host, redis_port, redis_password)
    //測試連接
    println(jedis.ping())

    val pool: JedisPool = connectRedisPool(redis_host)
    val jedis1: Jedis = pool.getResource
    jedis1.auth(redis_password)
    println(jedis1.ping())

    //存数据
    jedis.set("scala_redis_2004", "nice redis 111")
    //取数据
    println(jedis.get("scala_redis_2004"))

    //测试单个jedis
    val jedis2 = connectJedis(redis_host, redis_port, redis_password)
    println(jedis2.get("scala_redis_2004"))
  }
}
