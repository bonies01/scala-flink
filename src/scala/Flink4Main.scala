import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.{Instant, LocalDateTime, ZoneId}
import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 *
 * 时间语义
 */
object Flink4Main {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置水位线时间间隔
    env.getConfig.setAutoWatermarkInterval(1000)
    val source = env.addSource(new MySourceFunction)
    //有序流的watermark生成,开窗前必须先指定assign
     val assigner=   source.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps[Persion].withTimestampAssigner(
      new SerializableTimestampAssigner[Persion](){
        override def extractTimestamp(t: Persion, l: Long) = t.time
      }))

    val ke = assigner.map(p => (p.name, 1))
      .keyBy(key =>key._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)))
      .aggregate(new MyAggFunction,new MyProWindowFunction)
      ke.print

    env.execute
  }

}


class MyAggFunction extends AggregateFunction[Tuple2[String,Int],mutable.Map[String,Int],ListBuffer[String]]{
  override def createAccumulator():mutable.Map[String,Int] =new mutable.HashMap[String,Int]

  override def add(in: (String, Int), acc:mutable.Map[String,Int]): mutable.Map[String,Int] ={
    if(acc.contains(in._1)){
      acc += (in._1  -> (acc(in._1)+1))
    }
    else{
      acc += (in._1  ->1)
    }
    acc
  }

  override def getResult(acc: mutable.Map[String,Int]): ListBuffer[String] ={
    val list=acc.toList.sortBy(_._2).map(_._1)
    new ListBuffer[String]()++=list
  }

  override def merge(acc: mutable.Map[String,Int], acc1: mutable.Map[String,Int]): mutable.Map[String,Int] = ???
}

class MyProWindowFunction extends  ProcessWindowFunction[ListBuffer[String],String,String,TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[ListBuffer[String]], out: Collector[String]): Unit = {
      val list=elements.iterator.next
      val buf=new StringBuilder
     for(i<-0 until (if(list.size>10)10 else list.size)){
        buf ++="No." ++((i+1) toString)++ list(0)+","
      }
    out.collect(buf toString)
  }
}