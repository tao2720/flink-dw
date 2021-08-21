package com.qianfeng.assinger

import com.qianfeng.rdo.LogData.ODSLogDetail
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
 * 日志行为中的时间水印分配
 * @param maxLateness
 */
class LogPeriodicAssigner(maxLateness:Int) extends AssignerWithPeriodicWatermarks[ODSLogDetail]{

  var maxTimeStamp:Long = Integer.MIN_VALUE
  override def getCurrentWatermark: Watermark = new Watermark(maxTimeStamp-maxTimeStamp)

  //获取当前时间
  override def extractTimestamp(element: ODSLogDetail, previousElementTimestamp: Long): Long = {
    //获取当前最大时间戳
    maxTimeStamp = Math.max(maxTimeStamp,element.ct)
    element.ct
  }
}
