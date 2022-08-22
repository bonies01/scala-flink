import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
 *
 * connect 连接流
 */
object Flink10Main {
  def main(args: Array[String]): Unit = {
       val env=StreamExecutionEnvironment.getExecutionEnvironment
      val source1=env.fromElements[Long](1,2,3,4,5,6)
      val source2=env.fromElements[String]("100","101","102","103","104")
      val connect=source1.connect(source2)
     connect.map(new MyCoMapFunction).print
    env.execute
  }
}

class MyCoMapFunction extends CoMapFunction[Long,String,Int]{


  override def map1(in1: Long): Int = {
    in1 toInt
  }

  override def map2(in2: String): Int ={
     in2 toInt
  }
}
