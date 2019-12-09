package com.jackie.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

class StreamingFromCollectionScala {

  def main(args:Array[String]):Unit = {
    var env = StreamExecutionEnvironment.getExecutionEnvironment;

    //隐式转换
    import  org.apache.flink.api.scala._
    val  data = List(10,15,20);
    val text = env.fromCollection(data)

   val num = text.map(_+1)

    num.print().setParallelism(1);
    env.execute("name")

  }

}
