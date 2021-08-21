package com.qianfeng.etl.log

import com.qianfeng.assinger.LogPeriodicAssigner
import com.qianfeng.constant.Constants
import com.qianfeng.func.LogFunc.ViewListFunc
import com.qianfeng.rdo.LogData
import com.qianfeng.schema.{LogDetailDesSchema, ViewListLogsSerSchema}
import com.qianfeng.sink.ViewListSink
import com.qianfeng.util.{FlinkHelper, FlinkKafkaUtil}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

/**
 * 用户行为产品列表浏览明细
 */
object ViewListDataHandler {
  def handleViewListAction2Kafka(from_topic:String,groupID:String,to_topic:String,indexName:String): Unit ={
    //获取env
    val env: StreamExecutionEnvironment = FlinkHelper.getEnv()
    //env.enableCheckpointing(30*60*1000L)
    //获取消费订单的明细数据
    import org.apache.flink.api.scala._
    val viewListDS: DataStream[LogData.DWDViewListDetail] = env.addSource(FlinkKafkaUtil.createKafkaConsumerSchema(from_topic, groupID, new LogDetailDesSchema))
      .assignTimestampsAndWatermarks(new LogPeriodicAssigner(1000 * 10))
      //过滤数据源
      .filter(log => log.action.equals(Constants.ACTION_INTER)
        && (log.eventType.equals(Constants.EVENT_VIEW) || log.eventType.equals(Constants.EVENT_Slide)))
      .flatMap(new ViewListFunc)

    //1、将结果写入到kafka中
    val kafkaProducer: FlinkKafkaProducer[LogData.DWDViewListDetail] = FlinkKafkaUtil.createKafkaProducerSchema(to_topic, new ViewListLogsSerSchema(to_topic))
    viewListDS.print("---")
    viewListDS.addSink(kafkaProducer)


    //2、将结果同时写入ES中
    viewListDS.addSink(new ViewListSink(indexName))


    //触发执行
    env.execute("log-view-list")
  }

  //测试
  def main(args: Array[String]): Unit = {
    handleViewListAction2Kafka("tc_ods_logs","view_list_group","tc_dwd_view_list_detail","tc_view_list_detail")
  }
}
