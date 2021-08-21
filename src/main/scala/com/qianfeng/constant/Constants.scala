package com.qianfeng.constant

/**
 * 项目常量类
 */
object Constants {

  //常用数值常量
  val COMMON_ZERO_INT = 0
  val COMMON_ZERO_DOUBLE = 0.0
  val COMMON_ZERO_LONE = 0L

  val COMMON_ONE_INT = 1
  val COMMON_ONE_DOUBLE = 1.0
  val COMMON_ONE_LONE = 1L

  //失败重启策略
  val MAX_FAILUR_RESTART_ACCEPTES = 3
  val MAX_DELAY_INTERVAL = 5  //5秒

  //checkpoint监测点
  val CHECKPOINT_INTERVAL = 10

  //水印插入间隔
  val WATERMARK_INTERVAL = 500  //默认是500毫秒插入一次

  //当前可用线程数 --->获取运行时的最大可用线程
  val MAX_CURRENT_PARALLE = Runtime.getRuntime.availableProcessors()

  //kafka的配置信息
  val KAFKA_CONSUMER_CONFIG_URL = "kafka/kafka-consumer.properties"
  val KAFKA_PRODUCER_CONFIG_URL = "kafka/kafka-producer.properties"
  val KAFKA_GROUP_ID = "group.id"


  //redis的信息
  val REDIS_CONFIG_URL = "redis/redis.properties"
  val REDIS_HOST = "redis_host"
  val REDIS_PORT = "redis_port"
  val REDIS_PASS = "redis_password"
  val REDIS_TIMEOUT = "redis_timeout"
  val REDIS_DB = "redis_db"
  val REDIS_MAX_IDLE = "redis_maxidle"
  val REDIS_MIN_IDLE = "redis_minidle"
  val REDIS_MAX_TOTAL = "redis_maxtotal"


  //es的配置
  val ES_CONFIG_URL = "es/es-config.json"





}
