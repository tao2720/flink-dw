package com.qianfeng.func

import java.util

import com.qianfeng.JsonUtil
import com.qianfeng.constant.Constants
import com.qianfeng.rdo.OrderData.ODSOrderDetail
import org.apache.flink.api.common.functions.MapFunction

/**
 * 订单业务相关函数
 */
object OrderFunc {

  class OrderDetailFunc extends MapFunction[String,ODSOrderDetail]{
    override def map(value: String): ODSOrderDetail = {
      //将json字符串转换成map
      //将value转换成json对象
      val orderdata: util.Map[String, Object] = JsonUtil.gJson2Map(value)
      //将json中的key取出来，并对空进行处理
      val has_activity: String = orderdata.getOrDefault(Constants.ORDER_HAS_ACTIVITY,"").toString
      val ct: Long = orderdata.get(Constants.ORDER_ORDER_CT).toString.toLong
      var orderID: String = orderdata.get(Constants.ORDER_ORDER_ID).toString
      val fee: Double = orderdata.get(Constants.ORDER_PRODUCT_FEE).toString.toDouble
      val price: Double = orderdata.get(Constants.ORDER_PRODUCT_PRICE).toString.toDouble
      val productID: String = orderdata.get(Constants.ORDER_PRODUCT_ID).toString
      val productPub: String = orderdata.get(Constants.ORDER_PRODUCT_PUB).toString
      val traffic: String = orderdata.get(Constants.ORDER_PRODUCT_TRAFFIC).toString
      val traffic_grade: String = orderdata.get(Constants.ORDER_PRODUCT_TRAFFIC_GRADE).toString
      val traffic_type: String = orderdata.get(Constants.ORDER_PRODUCT_TRAFFIC_TYPE).toString
      val adult: Int = orderdata.get(Constants.ORDER_TRAVEL_MEMBER_ADULT).toString.toInt
      val yonger: Int = orderdata.get(Constants.ORDER_TRAVEL_MEMBER_YONGER).toString.toInt
      val baby: Int = orderdata.get(Constants.ORDER_TRAVEL_MEMBER_BABY).toString.toInt
      val userID: String = orderdata.get(Constants.ORDER_USER_ID).toString
      val userMobile: String = orderdata.get(Constants.ORDER_USER_MOBILE).toString
      val userRegion: String = orderdata.get(Constants.ORDER_USER_REGION).toString

      //清洗规则
      if(orderID.length < 27){
       /* for(i<-orderID.length to 27){
          orderID += "0"
        }*/
        orderID += "0"*(27-orderID.length)  //右补0
      }


      //构造返回
      ODSOrderDetail(orderID,userID,productID,productPub,userMobile,
        userRegion,traffic,traffic_grade,traffic_type,price,fee,
        has_activity,adult,yonger,baby,ct)
    }
  }

}
