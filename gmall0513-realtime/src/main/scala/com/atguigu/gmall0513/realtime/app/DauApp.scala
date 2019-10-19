package com.atguigu.gmall0513.realtime.app

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0513.common.constants.GmallConstant
import com.atguigu.gmall0513.realtime.bean.StartUpLog
import com.atguigu.gmall0513.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {

  //  1  消费kafka
  //    2  整理一下数据结构  string json  =>  case class
  //  3  根据清单进行过滤
  //    4  把用户访问清单保存到redis中
  //
  //  5  保存真正数据库(hbase)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_STARTUP, ssc)

    //       inputDstream.foreachRDD{rdd=>
    //         println(rdd.map(_.value()).collect().mkString("\n"))
    //       }
    //  转换格式 同时补充两个时间字段
    val startUplogDstream: DStream[StartUpLog] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
      val formator = new SimpleDateFormat("yyyy-MM-dd HH")
      val dateHour: String = formator.format(new Date(startUpLog.ts))
      val dateHourArr: Array[String] = dateHour.split(" ")
      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)
      startUpLog
    }
    //3 根据清单进行过滤
    //driver
    val filteredDstream: DStream[StartUpLog] = startUplogDstream.transform { rdd =>
      //driver 每批次执行一次
      println("过滤前：" + rdd.count())
      val jedis = RedisUtil.getJedisClient // driver
      val dateString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val dauKey = "dau:" + dateString
      val dauMidSet: util.Set[String] = jedis.smembers(dauKey)
      val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)

      val filteredRdd: RDD[StartUpLog] = rdd.filter { startuplog => //executor
        val dauMidSet: util.Set[String] = dauMidBC.value
        val flag: Boolean = dauMidSet.contains(startuplog.mid)
        !flag

      }
      println("过滤后：" + filteredRdd.count())
      filteredRdd

    }
     //批次内去重 ， 同一批次内，相同mid 只保留第一条 ==> 对相同的mid进行分组，组内进行比较 保留第一条
    val startupDstreamGroupbyMid: DStream[(String, Iterable[StartUpLog])] = filteredDstream.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()
    val startupRealFilteredDstream: DStream[StartUpLog] = startupDstreamGroupbyMid.flatMap { case (mid, startupItr) =>
      val top1List: List[StartUpLog] = startupItr.toList.sortWith { (startup1, startup2) =>
        startup1.ts < startup2.ts
      }.take(1)
      top1List
    }




    //  val jedis = RedisUtil.getJedisClient  // driver
    //  val dateString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    //  val dauKey="dau:"+dateString
    //  val dauMidSet: util.Set[String] = jedis.smembers(dauKey)
    //  val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)
    //
    //  startUplogDstream.filter{startuplog=>  //executor
    //    val dauMidSet: util.Set[String] = dauMidBC.value
    //    val flag: Boolean = dauMidSet.contains(startuplog.mid)
    //    !flag

    //    val jedis = RedisUtil.getJedisClient  //
    //    val dauKey="dau:"+startuplog.logDate
    //    val flag: lang.Boolean = jedis.sismember(dauKey,startuplog.mid)
    //    !flag



  //4  把用户访问清单保存到redis中
    startupRealFilteredDstream.foreachRDD { rdd =>

    rdd.foreachPartition { startupItr =>
      //executor 执行一次
      val jedis = RedisUtil.getJedisClient //driver
      for (startup <- startupItr) {
        println(startup)
        //executor  反复执行
        val dauKey = "dau:" + startup.logDate
        jedis.sadd(dauKey, startup.mid)
      }
      jedis.close()
    }
  }

//    rdd.foreach(startuplog=>{
//      //executor
//      val jedis = new Jedis("hadoop1",6379)//driver
//
//      //保存redis的操作   //所有今天访问过的mid的清单
//      //   redis  1 type:set    2 key: [dau:2019-10-19]  3 value: [mid]
//         val dauKey="dau:"+startuplog.logDate
//         jedis.sadd(dauKey,startuplog.mid)
//      jedis.close()
//    })

    // hbase  可以保存



      ssc.start()
      ssc.awaitTermination()

  }

}
