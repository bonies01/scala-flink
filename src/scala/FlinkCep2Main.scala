import java.time.Duration
import java.util

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
/**
 *
 * flink -dep
 */
object FlinkCep2Main {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source: DataStream[Persion1] = env.addSource[Persion1](new MySource1)
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness(Duration.ZERO)
        .withTimestampAssigner(new SerializableTimestampAssigner[Persion1] {
          override def extractTimestamp(t: Persion1, l: Long): Long = t.time
        }))
    //定义模式
    val pattern: Pattern[Persion1, Persion1] = Pattern.begin("match")
      .where(new SimpleCondition[Persion1] {
        override def filter(t: Persion1): Boolean = t.age > 4
      })
//      .next("second")
//      .where(new SimpleCondition[Persion1] {
//        override def filter(t: Persion1): Boolean = t.age > 6
//      })
//      .next("three")
//      .where(new SimpleCondition[Persion1] {
//        override def filter(t: Persion1): Boolean = t.age > 8
//      })
      //严格近邻3次
      .times(3).consecutive
    //将模式应用于数据流上,监测复杂事件
    val patternStream=CEP.pattern(source.keyBy(k=>k.name), pattern)
    //将监测到的复杂事件监测出来,进行处理得到报警信息
//    patternStream.select(new PatternSelectFunction[Persion1,String] {
//      override def select(map: util.Map[String, util.List[Persion1]]): String = {
//       val first= map.get("first").get(0)
//        val second= map.get("second").get(0)
//        val three= map.get("three").get(0)
//        s"${first}, ${second}, ${three}"
//      }
//    }).print
    patternStream.process(new PatternProcessFunction[Persion1,String] {
      override def processMatch(map: util.Map[String, util.List[Persion1]], context: PatternProcessFunction.Context, collector: Collector[String]): Unit ={
          //提取三次事件
          val list=map.get("match")
          val first= list.get(0)
          val second= list.get(1)
          val three= list.get(2)
          collector.collect(s"${first}, ${second}, ${three}")

      }
    }).print
    env.execute
  }
}
