package com.atguigu.app

import com.atguigu.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by WSC on 2021/10/30 10:50
 */
class DauApp {
  def main(args: Array[String]): Unit = {
//创建SparkConf并设置App名称
val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
//初始化SparkStreamingContext
val ssc = new StreamingContext(conf, Seconds(3))
MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

//启动
ssc.start()
//将主线程阻塞，主线程不退出保持任务执行
ssc.awaitTermination()

  }
}
