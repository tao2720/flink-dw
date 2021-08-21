package com.qianfeng.etl.order

import java.util.concurrent.TimeUnit

import com.qianfeng.assinger.OrdersPeriodicAssigner
import com.qianfeng.constant.Constants
import com.qianfeng.func.OrderFunc.OrderDetailFunc
import com.qianfeng.rdo.OrderData
import com.qianfeng.rdo.OrderData.ODSOrderDetail
import com.qianfeng.util.{FlinkHelper, FlinkKafkaUtil}
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * 将订单明细数据实时清洗后投递到HDFS中，一遍做离线、小时级、准实时的仓库
 * 清洗规则：
 *
 */
object OrderDetailDataHandler {

  /**
   * 消费数据落地hdfs中即可
   * @param fromTopic
   * @param groupID
   * @param outputPath
   */
  def handleOrderDetail2HDFS(fromTopic:String,groupID:String,outputPath:String): Unit = {
    //获取env
    val env: StreamExecutionEnvironment = FlinkHelper.getEnv()
    env.enableCheckpointing(30*60*1000L)
    //获取消费订单的明细数据
    import org.apache.flink.api.scala._
    val orderDetailDStream: DataStream[OrderData.ODSOrderDetail] = env.addSource(FlinkKafkaUtil.createKafkaConsumer(fromTopic, groupID))
      .map(new OrderDetailFunc)
      .assignTimestampsAndWatermarks(new OrdersPeriodicAssigner(5))
    //打印测试
    orderDetailDStream.print("---")

    //获取输出路径
    val output = new Path(outputPath)

    //分桶检查点时间间隔
    val bucketCheckInl = TimeUnit.SECONDS.toMillis(Constants.BUCKET_CHECK_INTERVAL)

    //数据分桶分配器 yyyyMMDDHH
    val bucketAssigner :BucketAssigner[ODSOrderDetail,String] = new DateTimeBucketAssigner(Constants.DATA_FORMAT_YYYYMMDDHH)

    //part文件格式配置
    /*val config = OutputFileConfig.builder()
      .withPartPrefix("part")
      .withPartSuffix(".parquet")
      .build()*/

    //4 数据实时采集落地
    //需要引入Flink-parquet的依赖
    val hdfsParquetSink: StreamingFileSink[ODSOrderDetail] = StreamingFileSink.forBulkFormat(output,
      ParquetAvroWriters.forReflectRecord(classOf[ODSOrderDetail]))
      .withBucketAssigner(bucketAssigner)
      .withBucketCheckInterval(bucketCheckInl)  //桶分配器检测间隔
      //.withOutputFileConfig(config)
      .build()
    //将数据流持久化到指定sink
    orderDetailDStream.addSink(hdfsParquetSink)

    //触发执行
    env.execute("handleData2Parquet")
  }

  //测试
  def main(args: Array[String]): Unit = {
    handleOrderDetail2HDFS("tc_ods_orders","order_group_id","hdfs://hadoop01:9000/tc/ods/orders/")
  }
}
