package com.qianfeng.schema

import com.qianfeng.JsonUtil
import com.qianfeng.rdo.LogData.{DWDClickDetail, ODSLogDetail}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * 点击行为明细反序列化
 */
class LogDetailDesSchema extends KafkaDeserializationSchema[ODSLogDetail]{
  override def isEndOfStream(nextElement: ODSLogDetail): Boolean = false

  //反序列化
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): ODSLogDetail = {
    //将record转换成字符串
    val line = new String(record.value())
    //将整行解析成对象 --- 需要默认构造器器
    //val logDetail = JsonUtil.json2object(line, classOf[ODSLogDetail])
    //logDetail

    val detail: java.util.Map[String, AnyRef] = JsonUtil.gJson2Map(line)
    //封装返回对象
    new ODSLogDetail(
      detail.get("os").toString,
      detail.get("lonitude").toString.toDouble,
      detail.get("latitude").toString.toDouble,
      detail.get("userRegion").toString,
      detail.get("eventType").toString,
      detail.get("userID").toString,
      detail.get("sid").toString,
      detail.get("manufacturer").toString,
      detail.get("duration").toString.toDouble,
      detail.get("carrier").toString,
      detail.get("userRegionIP").toString,
      detail.get("userDeviceType").toString,
      detail.get("action").toString,
      detail.get("userDevice").toString,
      if(detail.get("hotTarget") == null) "" else detail.get("hotTarget").toString,
      detail.get("networkType").toString,
      detail.get("exts").toString,
      BigDecimal.apply(detail.get("ct").toString).longValue()
    )
  }

  override def getProducedType: TypeInformation[ODSLogDetail] = {
    TypeInformation.of(classOf[ODSLogDetail])
  }
}
