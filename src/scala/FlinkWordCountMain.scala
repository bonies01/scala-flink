import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.Random
//case class Persion(name:String,age:Int)
class MySource extends SourceFunction[Persion]{
  var running=true
  override def run(sourceContext: SourceFunction.SourceContext[Persion]): Unit = {
    val random=new Random
    var i=0
    while(running){
      i=random.nextInt(10)
      sourceContext.collect(Persion("n"+i,i,""))
      TimeUnit.SECONDS.sleep(1)
    }
  }

  override def cancel(): Unit = running=false
}


object FlinkWordCountMain {
  def main(args: Array[String]): Unit = {
    val  env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val source=env.addSource(new MySource())
    val ke =source.map(p=>(p.name,1)).keyBy(k=>k._1).sum(1)
    ke.print
    ke.keyBy(k=>"key").max(1)
    env.execute

  }
}



