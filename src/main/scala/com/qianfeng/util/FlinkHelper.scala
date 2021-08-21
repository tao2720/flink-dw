package com.qianfeng.util

import com.qianfeng.constant.Constants
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 获取Flink的流逝env对象
 */
object FlinkHelper {
  /**
   * 获取流式的执行环境
   */
  def getEnv(parallel:Int,tc:TimeCharacteristic): StreamExecutionEnvironment ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallel) //设置并行度
    //设置失败重启
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Constants.MAX_FAILUR_RESTART_ACCEPTES,Constants.MAX_DELAY_INTERVAL))
    //设置checkpoint
    env.enableCheckpointing(Constants.CHECKPOINT_INTERVAL,CheckpointingMode.EXACTLY_ONCE)

    //设置env的时间类型
    env.setStreamTimeCharacteristic(tc)
    //设置水位插入间隔时间
    env.getConfig.setAutoWatermarkInterval(Constants.WATERMARK_INTERVAL)
    env
  }

  //根据并行度获取env
  def getEnv(parallel:Int): StreamExecutionEnvironment ={
    getEnv(parallel,TimeCharacteristic.EventTime)
  }


  //默认给最大并行度
  def getEnv():StreamExecutionEnvironment = {
    getEnv(Constants.MAX_CURRENT_PARALLE,TimeCharacteristic.EventTime)
  }

  //测试
  def main(args: Array[String]): Unit = {
    print(getEnv())
  }
}
