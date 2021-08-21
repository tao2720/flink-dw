package com.qianfeng.etl.log

import com.qianfeng.constant.Constants
import com.qianfeng.func.LogFunc.{ClickLogFunc, PageViewFunc}
import com.qianfeng.rdo.LogData
import com.qianfeng.rdo.LogData.DWDClickDetail
import com.qianfeng.schema.{LogDetailDesSchema, LogDetailSerSchema, PageViewDetailSerSchema}
import com.qianfeng.util.{FlinkHelper, FlinkKafkaUtil}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

/**
 * 处理用户行为中页面浏览明细明细数据清洗，并投递kafka中。
 *
 */
object PageViewDataHandler {
  /**
   *
   * @param from_topic
   * @param groupID
   * @param to_topic
   */
  def handlePageViwAction2Kafka(from_topic:String,groupID:String,to_topic:String): Unit ={
    //获取env
    val env: StreamExecutionEnvironment = FlinkHelper.getEnv()

    //获取消费log的明细数据
    import org.apache.flink.api.scala._
    val pageViewDetailDS = env.addSource(FlinkKafkaUtil.createKafkaConsumerSchema(from_topic, groupID, new LogDetailDesSchema))
      //过滤数据源
      .filter(log => log.action.equals(Constants.ACTION_NATIVE_PAGE_VIEW)
        || log.eventType.equals(Constants.ACTION_H5_PAGE_VIEW))
      .map(new PageViewFunc)

    //将结果写入到kafka中
    //1、需要编写一个kafka的分区器
    //2、需要编写kafka的序列化类
    val kafkaProducer: FlinkKafkaProducer[LogData.DWDPageViewDetail] = FlinkKafkaUtil.createKafkaProducerSchema(to_topic, new PageViewDetailSerSchema(to_topic))
    kafkaProducer.setWriteTimestampToKafka(true)
    pageViewDetailDS.print("===")
    pageViewDetailDS.addSink(kafkaProducer)

    //触发执行
    env.execute("log-click")
  }

  //测试
  def main(args: Array[String]): Unit = {
    handlePageViwAction2Kafka("tc_ods_logs","page_view_group","tc_dwd_page_view_detail")
  }
}
