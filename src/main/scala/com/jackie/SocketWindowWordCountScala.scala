package com.jackie

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object SocketWindowWordCountScala {

  def main(args: Array[String]): Unit = {
    //获取运行环境

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;

    //连接socket 获取输入数据
    val hostname: String = "192.168.3.130";
    val port: Int = 9000;
    val delimiter: Char = '\n';

    val text = env.socketTextStream(hostname, port, delimiter);
// 必须要添加一行隐式转，不然编译失败
    import org.apache.flink.api.scala._
    //解析数据， 分组， 窗口计算， 聚合求sum
    val windowCount = text.flatMap(line => line.split("\\s"))
      .map(w => WordWithCount(w, 1)) // 把装为word 和1
      .keyBy("word")
      .timeWindow(Time.seconds(2), Time.seconds(1))
      //    .sum("count");
      .reduce((a, b) => WordWithCount(a.word, a.count + b.count))
    windowCount.print().setParallelism(1);
    env.execute("Socket Window Count")
  }


  case class WordWithCount(word: String, count: Long) {

  }

}
