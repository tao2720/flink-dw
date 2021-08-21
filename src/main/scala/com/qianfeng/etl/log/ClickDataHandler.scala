package com.qianfeng.etl.log

import com.qianfeng.constant.Constants
import com.qianfeng.func.LogFunc.ClickLogFunc
import com.qianfeng.rdo.LogData
import com.qianfeng.rdo.LogData.{DWDClickDetail, ODSLogDetail}
import com.qianfeng.schema.{LogDetailDesSchema, LogDetailSerSchema}
import com.qianfeng.util.{FlinkHelper, FlinkKafkaUtil}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

/**
 * 处理用户行为中点击明细数据清洗，并投递kafka中。
 *
 */
object ClickDataHandler {
  /**
   *
   * @param from_topic
   * @param groupID
   * @param to_topic
   */
  def handleClickAction2Kafka(from_topic:String,groupID:String,to_topic:String): Unit ={
    //获取env
    val env: StreamExecutionEnvironment = FlinkHelper.getEnv()

    //获取消费订单的明细数据
    import org.apache.flink.api.scala._
    val clickDetailDS:DataStream[DWDClickDetail] = env.addSource(FlinkKafkaUtil.createKafkaConsumerSchema(from_topic, groupID, new LogDetailDesSchema))
      //过滤数据源
      .filter(log => log.action.equals(Constants.ACTION_INTER)
        && log.eventType.equals(Constants.EVENT_CLICK))
      .map(new ClickLogFunc)
    clickDetailDS.print("===")

    //将结果写入到kafka中
    //1、需要编写一个kafka的分区器
    //2、需要编写kafka的序列化类
    val kafkaProducer: FlinkKafkaProducer[LogData.DWDClickDetail] = FlinkKafkaUtil.createKafkaProducerSchema(to_topic, new LogDetailSerSchema(to_topic))
    kafkaProducer.setWriteTimestampToKafka(true)
    //kafkaProducer.setProperty("transaction.timeout.ms",1000*60*5+"")
    clickDetailDS.print("===")
    clickDetailDS.addSink(kafkaProducer)

    //触发执行
    env.execute("log-click")
  }

  //测试
  def main(args: Array[String]): Unit = {
    handleClickAction2Kafka("tc_ods_logs","click_group","tc_dwd_click_detail")
  }
}
