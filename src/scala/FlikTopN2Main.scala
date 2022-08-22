import java.lang
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
object FlikTopN2Main {
  def main(args: Array[String]): Unit = {
    val  env=StreamExecutionEnvironment.getExecutionEnvironment
    //设置watermark时间间隔，默认200
    //env.getConfig.setAutoWatermarkInterval(5000)

    val source=env.addSource(new MySource1)
    source.assignTimestampsAndWatermarks(WatermarkStrategy
      .forBoundedOutOfOrderness[Persion1](Duration.ZERO)
      .withTimestampAssigner(new SerializableTimestampAssigner[Persion1] {
      override def extractTimestamp(t: Persion1, l: Long): Long = t.age
    }) )
     val ke= source.keyBy(p=>p.age)
      ke.process(new MyProcess2Function).print
    env.execute
  }

}

class MyProcess2Function extends KeyedProcessFunction[Int,Persion1,String]{
  var stateList:ListState[Persion1] =null;
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val desc=new ListStateDescriptor[Persion1]("list-state",Types.CASE_CLASS[Persion1])
    val sl=getRuntimeContext.getListState(desc)
     this.stateList=sl
  }
  override def processElement(i:Persion1, context: KeyedProcessFunction[Int,Persion1, String]#Context, collector: Collector[String]): Unit = {

    //将数据添加到状态中
     stateList.add(i)
     //注册定时器 +1,context.getCurrentKey
    context.timerService().registerEventTimeTimer(context.getCurrentKey+1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Int ,Persion1, String]#OnTimerContext, out: Collector[String]): Unit = {
    val  iterator=stateList.get().iterator
     while(iterator.hasNext){
       out.collect(iterator.next.name)
     }
  }
}
