import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.PreparedStatement
import java.time.temporal.TemporalQueries.zoneId
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util
import java.util.Random
import java.util.concurrent.TimeUnit
import scala.collection.mutable

/**
 *
 * 时间语义
 */
object Flink3Main {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置水位线时间间隔
    env.getConfig.setAutoWatermarkInterval(500)
    val source = env.addSource(new MySourceFunction)
    //有序流的watermark生成,开窗前必须先指定assign
     val assigner=   source.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps[Persion].withTimestampAssigner(
      new SerializableTimestampAssigner[Persion](){
        override def extractTimestamp(t: Persion, l: Long) = t.time
      }))
    //测输出流
    val outTag=new OutputTag[(String, Int)]("tag1")
    val ke = assigner.map(p => (p.name, 1))
      .keyBy(key =>true)
      .window(TumblingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)))
      .allowedLateness(Time.seconds(5))//迟到数据
      .sideOutputLateData(outTag)
      .process(new MyProcessFunction)
      ke.print
      ke.getSideOutput(outTag).print("侧输出流")
    env.execute
  }

}

/**
 * 自定义的processWindowFunction，输出一条统计信息
 */
class MyProcessFunction extends  ProcessWindowFunction[Tuple2[String,Int],String,Boolean,TimeWindow]{
  override def process(key: Boolean, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit ={
        val set=new util.HashSet[String]
        for(e <- elements){
          set.add(e._1)
        }
      val size=set.size
      val start=context.window.getStart
      val st=  LocalDateTime.ofInstant(Instant.ofEpochMilli(start),ZoneId.systemDefault())
      val end=context.window.getEnd
    val ed=  LocalDateTime.ofInstant(Instant.ofEpochMilli(end),ZoneId.systemDefault())
    val msg=s"开始时间:${st},结束时间:${ed},uv:${size}"
    out.collect(msg)
  }
}

