import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
 * 测输出流
 *
 */
object FlikTopN3Main {
  def main(args: Array[String]): Unit = {
    val  env=StreamExecutionEnvironment.getExecutionEnvironment
    //设置watermark时间间隔
    env.getConfig.setAutoWatermarkInterval(5000)

    val source=env.addSource(new MySource1)
    val tag1=new OutputTag[Persion1]("tag1")
    val tag2=new OutputTag[Persion1]("tag2")
    source.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Persion1](Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner[Persion1] {
      override def extractTimestamp(t: Persion1, l: Long): Long = t.time
    }) )
     val ke= source.process(new MyProcess3Function(tag1,tag2))
    ke.print("主流----")
    ke.getSideOutput[Persion1](tag1).print("<5流")
    ke.getSideOutput[Persion1](tag2).print("<7流")
    env.execute
  }

}

class MyProcess3Function(tag1: OutputTag[Persion1],tag2: OutputTag[Persion1]) extends ProcessFunction[Persion1,Persion1]{

  override def processElement(i: Persion1, context: ProcessFunction[Persion1, Persion1]#Context, collector: Collector[Persion1]): Unit = {
    val age=i.age
    if(age<=5){
      context.output[Persion1](tag1,i)
    }
    else if(age<=7){
      context.output[Persion1](tag1,i)
    }
    else{
      collector.collect(i)
    }
  }
}



