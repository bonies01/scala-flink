import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.functions.{PatternProcessFunction, TimedOutPartialMatchHandler}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.cep.scala.CEP
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.time.{Instant, LocalDateTime, ZoneId}
import java.util
object FlinkCepMain {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置水位线时间间隔
    env.getConfig.setAutoWatermarkInterval(500)
    val source: DataStream[Persion] = env.addSource(new MySourceFunction)
    //有序流的watermark生成,开窗前必须先指定assign
    val assigner=   source.assignTimestampsAndWatermarks(WatermarkStrategy
      .forMonotonousTimestamps[Persion]
      .withTimestampAssigner(
      new SerializableTimestampAssigner[Persion](){
        override def extractTimestamp(t: Persion, l: Long) = t.time
      }))
    val pat: Pattern[Persion, Persion] =Pattern.begin("create")
      .where(new SimpleCondition[Persion] {
        override def filter(t: Persion) = t.opera.equals("create")
      })
      .followedBy("pay")
      .where(new SimpleCondition[Persion] {
        override def filter(t: Persion) = t.opera.equals("pay")
      })
      .within(Time.minutes(5l))
     val cep= CEP.pattern(source.keyBy(k=>k.name),pat)
      val out=new OutputTag[String]("timeOut")
     val process: DataStream[String] = cep.process(new MyCepProcessFunction)
    process.print
    process.getSideOutput(out)
    env.execute
  }

}

class MyCepProcessFunction extends PatternProcessFunction[Persion,String] with TimedOutPartialMatchHandler[Persion]{

  override def processMatch(map: util.Map[String, util.List[Persion]], context: PatternProcessFunction.Context, collector: Collector[String]): Unit = {
      val list=map.get("pay")
        val persion=list.get(0)
       val time=LocalDateTime.ofInstant( Instant.ofEpochMilli(persion.time),ZoneId.systemDefault())
       collector.collect(s"支付->${persion.name},${persion.opera},${time}")
  }

  override def processTimedOutMatch(map: util.Map[String, util.List[Persion]], context: PatternProcessFunction.Context): Unit = {
    val list=map.get("create")
    val persion=list.get(0)
    val time=LocalDateTime.ofInstant( Instant.ofEpochMilli(persion.time),ZoneId.systemDefault())
    val out=new OutputTag[String]("timeOut")
     context.output[String](out,s"超时->${persion.name},${persion.opera},${time}")
  }
}