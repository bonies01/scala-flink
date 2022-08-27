import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import java.util


/**
 *
 * 时间语义
 */
object Flink2Main {
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
    val ke = assigner.map(p => (p.name, 1))
      .keyBy(key => key._1)
      //滚动窗口
//      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      //滑动窗口
      .window(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)))
      .aggregate(new myAggregate)

    ke.print
    env.execute
  }

}

/**
 * pv,uv 统计
 */
class myAggregate extends AggregateFunction[Tuple2[String,Int],Tuple2[Long,util.HashSet[String]],Double]{
  override def createAccumulator(): (Long, util.HashSet[String]) = {
    (0L,new util.HashSet[String])
  }

  override def add(in: (String, Int), acc: (Long, util.HashSet[String])): (Long, util.HashSet[String]) ={
      acc._2.add(in._1)
    //没来一条数据pv个数+1，将key放入hashset
     (acc._1+1,acc._2)
  }

  override def getResult(acc: (Long, util.HashSet[String])): Double = {
         acc._1 / acc._2.size toDouble
  }
  override def merge(acc: (Long, util.HashSet[String]), acc1: (Long, util.HashSet[String])): (Long, util.HashSet[String]) = ???

}