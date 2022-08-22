import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.api.scala._
import java.sql.PreparedStatement
import java.util.Random
import java.util.concurrent.TimeUnit

object FlinkMain {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.addSource(new MySourceFunction)
    val ke= source.map(new MyMapFunction).keyBy(p=>p._1).sum(1)
      //.returns(Types.TUPLE(Types.STRING,Types.INT)).keyBy(p=>p._1).sum(2)
    /*.returns(Types.TUPLE(Types.STRING,Types.INT))*/
    // ke.print
    //写入文件
    //   val fileSink= StreamingFileSink.forRowFormat(new Path("d://out.txt"),
    //     new SimpleStringEncoder[Persion]("UTF-8"))
    //     .withRollingPolicy(DefaultRollingPolicy.builder()
    //     .withMaxPartSize(1024*1024*1024)
    //     .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
    //       .withInactivityInterval(15).build()
    //     ).build()
    //    ke.addSink(fileSink)
    //写入Oracle
    val sql = "insert into test_01 (key,cnt)values(?,?)"
    //   val jdbcSink= JdbcSink.sink[Persion](sql,
    //     (statemnet:PreparedStatement,event:Persion)=>{
    //        statemnet.setString(1,event.name)
    //        statemnet.setInt(2,event.age )
    //    },new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
    val jdbcSink = JdbcSink.sink[Tuple2[String,Int]](sql,
      new JdbcStatementBuilder[Tuple2[String,Int]] {
        override def accept(t: PreparedStatement, u: Tuple2[String,Int]): Unit = {
          t.setString(1, u._1)
          t.setInt(2, u._2)
        }
      },JdbcExecutionOptions.builder.withBatchSize(1).build,
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:oracle:thin:@192.168.122.128:1521/helowin")
        .withDriverName("oracle.jdbc.driver.OracleDriver")
        .withUsername("orcl")
        .withPassword("123456")
        .build)

    ke.addSink(jdbcSink)
    ke.print
    env.execute
  }

}

case class Persion(name: String, age: Int,opera:String,time:Long=System.currentTimeMillis())

class MySourceFunction extends SourceFunction[Persion] {
  var running = true

  override def run(sourceContext: SourceFunction.SourceContext[Persion]): Unit = {
    val random = new Random()
    var i = 0
    val array=Array("create","pay","order","modify")
    while (running) {
      i = random.nextInt(10)
      sourceContext.collect(Persion("per" + i, i,array(i % 4)))
      TimeUnit.SECONDS.sleep(1)
    }
  }

  override def cancel(): Unit = running = false
}

class MyMapFunction extends MapFunction[Persion,Tuple2[String,Int]]{
  override def map(t: Persion):Tuple2[String,Int] = {
    (t.name,1)
  }
}

