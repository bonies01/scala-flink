import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
object Flink9Main {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val s1=env.fromElements(("n1","login"),("n2","pay"),("n3","order"),("n4","login"),("n1","login"))
    val s2=env.fromElements(("login","order"),("login","pay"))
    //定义广播
   val boardDesc= new MapStateDescriptor("k1", org.apache.flink.api.common.typeinfo.Types.VOID,Types.TUPLE[Tuple2[String,String]])
    val boardSource: BroadcastStream[(String, String)] =s2.broadcast(boardDesc)
    s1.keyBy(k=>k._1)
      .connect(boardSource)
      .process(new MyBoardProcessFunction)
      .print
    env.execute
  }
}

class MyBoardProcessFunction extends KeyedBroadcastProcessFunction[String,(String,String),(String,String),String]{

  //保存用户的上一个行为
  var valueState:ValueState[(String,String)] = null;

  override def open(parameters: Configuration): Unit = {
    val desc=new ValueStateDescriptor[(String,String)]("vk",Types.TUPLE[(String,String)])
    valueState=getRuntimeContext.getState[(String,String)](desc)
  }
  override def processElement(in1: (String, String), readOnlyContext: KeyedBroadcastProcessFunction[String, (String, String), (String, String), String]#ReadOnlyContext, collector: Collector[String]): Unit = {
    val boardDesc= new MapStateDescriptor("k1", org.apache.flink.api.common.typeinfo.Types.VOID,Types.TUPLE[Tuple2[String,String]])
    //从广播状态中获得匹配模式
    val board=readOnlyContext.getBroadcastState(boardDesc)
    //获取用户上一次滴行为
    valueState.value
    collector.collect(s"上一值:${valueState.value},当前值:${in1},board:${board.get(null)}")
    valueState.update(in1)
  }

  override def processBroadcastElement(in2: (String, String), context: KeyedBroadcastProcessFunction[String, (String, String), (String, String), String]#Context, collector: Collector[String]): Unit = {
    //从上下文中获取广播状态,并用当前数据更新状态
    val boardDesc= new MapStateDescriptor("k1", org.apache.flink.api.common.typeinfo.Types.VOID,Types.TUPLE[Tuple2[String,String]])
    val board: BroadcastState[Void, (String, String)] =context.getBroadcastState(boardDesc)
      board.put(null,in2)
  }
}
