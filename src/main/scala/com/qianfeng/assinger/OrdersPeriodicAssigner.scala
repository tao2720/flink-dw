package com.qianfeng.assinger

import com.qianfeng.rdo.OrderData.ODSOrderDetail
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
 * 为订单明细分配时间水印
 */
class OrdersPeriodicAssigner(maxLateness:Int) extends AssignerWithPeriodicWatermarks[ODSOrderDetail]{

  var maxTimeStamp:Long = Integer.MIN_VALUE
  override def getCurrentWatermark: Watermark = new Watermark(maxTimeStamp-maxTimeStamp)

  //获取当前时间
  override def extractTimestamp(element: ODSOrderDetail, previousElementTimestamp: Long): Long = {
    //获取当前最大时间戳
    maxTimeStamp = Math.max(maxTimeStamp,element.ct)
    element.ct
  }
}
