import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, LocalDateTime, ZoneId}

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
object FlinkTable1Main {
  def main(args: Array[String]): Unit = {
    val setting=EnvironmentSettings.newInstance().build
    val streamEnv=StreamExecutionEnvironment.getExecutionEnvironment
    val source: DataStream[Persion1] =streamEnv.addSource[Persion1](new MySource1)
//      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ZERO)
//          .withTimestampAssigner(new SerializableTimestampAssigner[Persion1](){
//            override def extractTimestamp(t: Persion1, l: Long): Long = t.time
//          })
//      )
//    source
//      .keyBy(k=>k.name)
//      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
//      .process(new MyProTunc)
//      .print
//    streamEnv.execute
    val tableEnv=  StreamTableEnvironment.create(streamEnv,setting)
    tableEnv.createTemporaryView("persion",source)
    val sql="""
        select name,sum(1) as cnt from persion
        group by name
        """.stripMargin
    tableEnv.sqlQuery(sql).execute.print


  }
}
//
//class MyProTunc extends ProcessWindowFunction[Persion1,String,String,TimeWindow]{
//
//  override def process(key: String, context: Context, elements: Iterable[Persion1], out: Collector[String]): Unit ={
//    val iterator=elements.iterator
//    while(iterator.hasNext){
//      val next=iterator.next
//        out.collect(s"key:${key},姓名:${next.name},时间:${LocalDateTime
//          .ofInstant(Instant.ofEpochMilli(next.time),ZoneId.systemDefault())
//          .format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"))}" )
//
//    }
//
//  }
//}
