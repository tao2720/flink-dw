package com.qianfeng.schema

import java.lang

import com.qianfeng.JsonUtil
import com.qianfeng.rdo.LogData.DWDClickDetail
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * 点击行为明细的序列化
 * @param toTopic
 */
class LogDetailSerSchema(toTopic:String) extends KafkaSerializationSchema[DWDClickDetail]{
  override def serialize(element: DWDClickDetail, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    //构造key
    val key = element.userID+"_"+element.sid
    val value:String = JsonUtil.gObject2Json(element)
    //构造返回
    new ProducerRecord[Array[Byte], Array[Byte]](toTopic,key.getBytes,value.getBytes)
  }
}
