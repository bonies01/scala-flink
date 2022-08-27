import java.time.{Duration, Instant}
import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, ProcessingTimeSessionWindows, SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
case class Persion1(name:String,age:Int,time:Long=Instant.now().toEpochMilli)
class MySource1 extends SourceFunction[Persion1]{
  var running=true
  override def run(sourceContext: SourceFunction.SourceContext[Persion1]): Unit = {
    val random=new Random
    var i=0
    while(running){
      i=random.nextInt(10)
      sourceContext.collect(Persion1("n"+i,i))
      TimeUnit.SECONDS.sleep(1)
    }
  }

  override def cancel(): Unit = running=false
}


object FlinkTopNMain {
  def main(args: Array[String]): Unit = {
    val  env=StreamExecutionEnvironment.getExecutionEnvironment
    //设置watermark时间间隔
    env.getConfig.setAutoWatermarkInterval(5000)
    import org.apache.flink.streaming.api.scala._
    val source=env.addSource(new MySource1)
    //设置watermark
//    source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Persion1](Time.seconds(1)) {
//      override def extractTimestamp(t: Persion1): Long = t.time
//    })
    source.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Persion1](Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner[Persion1] {
      override def extractTimestamp(t: Persion1, l: Long): Long = t.time
    }) )
    val ke =source.map(p=>(p.name,1))
      .keyBy(k=>k._1)
      //.window(EventTimeSessionWindows.withGap(Time.seconds(5)))//事件时间会话窗口
      //.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))//时间会话窗口
        //.window(TumblingEventTimeWindows.of(Time.seconds(5)))//事件事件滚动窗口
        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))//时间滚动窗口
        //.window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))//事件时间滑动窗口
      //.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))//时间滑动窗口
        .aggregate(new MyAggregatorFunction,new MyProcessFunction1)
    ke.print
    env.execute

  }
}

class MyAggregatorFunction extends AggregateFunction[Tuple2[String,Int],mutable.Map[String,Int],mutable.ListBuffer[String]]{
  override def createAccumulator(): mutable.Map[String, Int] = new mutable.HashMap[String,Int]()

  override def add(in: (String, Int), acc: mutable.Map[String, Int]): mutable.Map[String, Int] ={
    if(acc.contains(in._1)){
       acc += (in._1->(acc(in._1)+1))
    }
    else{
      acc += (in._1->1)
    }
    acc
  }

  override def getResult(acc: mutable.Map[String, Int]): ListBuffer[String] = {
      val list=acc.toList.sortBy(_._2).map(_._1)
      val buf =new ListBuffer[String]()
       buf ++= list
  }

  override def merge(acc: mutable.Map[String, Int], acc1: mutable.Map[String, Int]): mutable.Map[String, Int] = ???
}

class MyProcessFunction1 extends ProcessWindowFunction[ListBuffer[String],String,String,TimeWindow]{

  override def process(key: String, context: Context, elements: Iterable[ListBuffer[String]], out: Collector[String]): Unit = {
     val list =elements.iterator.next
    print("list:"+list)
     val buf=new mutable.StringBuilder

     for(i<-0 until (if(list.size>5)5 else list.size) ){
       print("i:"+i)
        buf++="No."+((i+1) toString)++":"++list(i)
     }
    out.collect(buf toString)
  }
}



