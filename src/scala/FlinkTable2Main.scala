import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.functions.{FunctionKind, ScalarFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.types.inference.TypeInference

object FlinkTable2Main {
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
    val table= tableEnv.from("persion")
    table.printSchema()
    //用户自定义udf
    tableEnv.createTemporarySystemFunction("selfFunc",new selfFunc)
    tableEnv.createTemporarySystemFunction("selfTableFunc",new SelfTableFunc)
//    val sql="""
//          select name,age, SELFFUNC(name) as self from persion
//        """.stripMargin
        val sql="""
              select name, SELFFUNC(name) as self
              from persion,LATERAL TABLE(SELFTABLEFUNC(name)) AS T(nam,ag,ti)

            """.stripMargin
    tableEnv.sqlQuery(sql).execute.print


  }
}

class selfFunc extends ScalarFunction{
  def eval(str:String):Int=str.hashCode
}

class SelfTableFunc extends TableFunction[Persion1]{
  def eval(str:String):Persion1={
    val sub=str.substring(0,1)
    Persion1(name=sub,age=sub.length)
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
