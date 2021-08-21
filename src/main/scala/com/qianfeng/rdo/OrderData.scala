package com.qianfeng.rdo

/**
 * 订单模型的封装
 */
object OrderData {

  /**
   * 订单明细数据
   */
  case class ODSOrderDetail(orderID:String, userID:String, productID:String, pubID:String,
                            userMobile:String, userRegion:String, traffic:String, trafficGrade:String, trafficType:String,
                            price:Double, fee:Double, hasActivity:String,
                            adult:Int, yonger:Int, baby:Int, ct:Long)

  case class HDFSOrderDetail(orderID:String, userID:String, productID:String, pubID:String,
                            userMobile:String, userRegion:String, traffic:String, trafficGrade:String, trafficType:String,
                            price:Double, fee:Double, hasActivity:String,
                            adult:Int, yonger:Int, baby:Int, year:Int,month:Int,week:Int,startOfWeek:String,endOfWeek:String,ct:Long)
}
