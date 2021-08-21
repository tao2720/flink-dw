package com.qianfeng.func

import java.util

import com.qianfeng.JsonUtil
import com.qianfeng.constant.Constants
import com.qianfeng.rdo.LogData.{DWDClickDetail, DWDPageViewDetail, DWDViewListDetail, ODSLogDetail}
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.util.Collector

/**
 * 行为业务相关自定义函数
 */
object LogFunc {

  /**
   * 将行为明细转换成点击明细
   */
  class ClickLogFunc extends MapFunction[ODSLogDetail,DWDClickDetail]{
    override def map(value: ODSLogDetail): DWDClickDetail = {
      //定义ext中两个字段,并给与默认值
      var targetID:String = ""
      var eventTargetType:String = ""
      //先判断ext是否存在
      if(value.ext != null){
        //将value转换成json对象
        val ext_json: util.Map[String, Object] = JsonUtil.gJson2Map(value.ext)
        targetID = ext_json.getOrDefault(Constants.LOG_TARGET_ID,"").toString
        eventTargetType = ext_json.getOrDefault(Constants.LOG_EVENT_TARGET_TYPE,"").toString
      }


      //封装返回
      DWDClickDetail(value.os,value.longitude,value.latitude,value.userRegion,value.eventType,
        value.userID,value.sid,value.manufacturer,value.duration,value.carrier,value.userRegionIP,
        value.userDeviceType,value.action,value.userDevice,value.networkType,targetID,eventTargetType,
        value.ct)
    }
  }

  class PageViewFunc extends MapFunction[ODSLogDetail,DWDPageViewDetail]{
    override def map(value: ODSLogDetail): DWDPageViewDetail = {
      //定义ext中两个字段,并给与默认值
      var targetID:String = ""
      //先判断ext是否存在
      if(value.ext != null){
        //将value转换成json对象
        val ext_json: util.Map[String, Object] = JsonUtil.gJson2Map(value.ext)
        targetID = ext_json.getOrDefault(Constants.LOG_TARGET_ID,"").toString
      }

      //封装返回
      DWDPageViewDetail(value.os,value.longitude,value.latitude,value.userRegion,value.eventType,
        value.userID,value.sid,value.manufacturer,value.duration,value.carrier,value.userRegionIP,
        value.userDeviceType,value.action,value.userDevice,value.networkType,targetID,
        value.ct)
    }
  }

  /**
   * 产品列表浏览函数
   */
  class ViewListFunc extends FlatMapFunction[ODSLogDetail,DWDViewListDetail]{
    override def flatMap(value: ODSLogDetail, out: Collector[DWDViewListDetail]): Unit = {
      //取出对应ext
      val ext_map: util.Map[String, AnyRef] = JsonUtil.gJson2Map(value.ext)
      if(ext_map != null){
        //取出targetIDS
        val targetIDs: String = ext_map.get(Constants.LOG_TARGET_IDS).toString
        val travelSendTime: String = ext_map.get(Constants.LOG_TRAVELSENDTIME).toString
        val travelTime: String = ext_map.get(Constants.LOG_TRAVELTIME).toString
        val productLevel: String = ext_map.get(Constants.LOG_PRODUCTLEVEL).toString
        val travelSend: String = ext_map.get(Constants.LOG_TRAVELSEND).toString
        val productType: String = ext_map.get(Constants.LOG_PRODUCTTYPE).toString

        //循环targetIDS []
        val targets_arr: Array[String] = targetIDs.replaceAll("\\[", "")
          .replaceAll("\\]", "")
          .split(",")

        //循环targetIDS数组
        for (targetID <- targets_arr){
          //封装返回
          val viewList = new DWDViewListDetail(value.os,value.longitude,value.latitude,value.userRegion,value.eventType,
            value.userID,value.sid,value.manufacturer,value.duration,value.carrier,value.userRegionIP,
            value.userDeviceType,value.action,value.userDevice,value.networkType,
            value.hotTarget,targetID.replaceAll("\"",""),travelSendTime,
            travelTime,productLevel,travelSend,productType,value.ct)
          //添加到out中
          out.collect(viewList)
        }
      }
    }
  }

}
