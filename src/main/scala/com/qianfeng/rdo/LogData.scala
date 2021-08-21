package com.qianfeng.rdo

/**
 * 用户行为数据模型封装
 */
object LogData {

  //用户行为最原始数据
  case class ODSLogDetail(os:String,longitude:Double,latitude:Double,userRegion:String,
                          eventType:String,userID:String,sid:String,manufacturer:String,
                          duration:Double,carrier:String,userRegionIP:String,userDeviceType:String,
                          action:String,userDevice:String,hotTarget:String,networkType:String,
                          ext:String,ct:Long)


  //用户点击明细数据
  case class DWDClickDetail(os:String,longitude:Double,latitude:Double,userRegion:String,
                          eventType:String,userID:String,sid:String,manufacturer:String,
                          duration:Double,carrier:String,userRegionIP:String,userDeviceType:String,
                          action:String,userDevice:String,networkType:String,
                          targetID:String,eventTargetType:String,ct:Long)

  //用户页面明细数据
  case class DWDPageViewDetail(os:String,longitude:Double,latitude:Double,userRegion:String,
                            eventType:String,userID:String,sid:String,manufacturer:String,
                            duration:Double,carrier:String,userRegionIP:String,userDeviceType:String,
                            action:String,userDevice:String,networkType:String,
                            targetID:String,ct:Long)

  //用户产品列表浏览明细
  case class DWDViewListDetail(os:String,longitude:Double,latitude:Double,userRegion:String,
                               eventType:String,userID:String,sid:String,manufacturer:String,
                               duration:Double,carrier:String,userRegionIP:String,userDeviceType:String,
                               action:String,userDevice:String,networkType:String,
                               hotTarget:String,targetID:String,travelSendTime:String,travelTime:String,
                               productLevel:String,travelSend:String,productType:String,ct:Long)



}
