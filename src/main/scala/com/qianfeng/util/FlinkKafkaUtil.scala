package com.qianfeng.util

import java.util.Properties

import com.qianfeng.constant.Constants
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.flink.api.scala._

/**
 * flink消费和生产kafka的数据的工具类
 */
object FlinkKafkaUtil {

  /**
   * 更加topic和groupic消费指定主题的数据
   * @param fromTopic
   * @param groupID
   */
  def createKafkaConsumer(fromTopic:String,groupID:String): FlinkKafkaConsumer[String] ={
    //获取propeties
    val prop:Properties = PropertiesUtil.readProperty(Constants.KAFKA_CONSUMER_CONFIG_URL)
    prop.setProperty(Constants.KAFKA_GROUP_ID,groupID)
    //1、topic  2、序列化类  3、properties
    new FlinkKafkaConsumer[String](fromTopic,new SimpleStringSchema(),prop)
  }


  /**
   * 根据topic和schema等信息指定消费并进行反序列化
   * @param fromTopic
   * @param groupID
   * @param schema
   * @tparam T
   * @return
   */
  def createKafkaConsumerSchema[T](fromTopic:String,groupID:String,schema:KafkaDeserializationSchema[T]): FlinkKafkaConsumer[T] ={
    //获取propeties
    val prop:Properties = PropertiesUtil.readProperty(Constants.KAFKA_CONSUMER_CONFIG_URL)
    prop.setProperty(Constants.KAFKA_GROUP_ID,groupID)
    //1、topic  2、序列化类  3、properties
    new FlinkKafkaConsumer[T](fromTopic,schema,prop)
  }


  /**
   * 根据topic和schema等信息指定消费并进行反序列化
   * @param schema
   * @tparam T
   * @return
   */
  def createKafkaProducerSchema[T](toTopic:String,schema:KafkaSerializationSchema[T]): FlinkKafkaProducer[T] ={
    //获取propeties
    val prop:Properties = PropertiesUtil.readProperty(Constants.KAFKA_PRODUCER_CONFIG_URL)
    //1、topic  2、序列化类  3、properties  -- 必须要指定语义
    new FlinkKafkaProducer[T](toTopic,schema,prop,FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
  }


  /**
   * 测试
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val orderConsumer = createKafkaConsumer("tc_ods_orders", "order_group_id")
    val env = FlinkHelper.getEnv()
    val ds = env.addSource(orderConsumer)
    ds.print("order detail")
    //执行
    env.execute("order detail")
  }
}
