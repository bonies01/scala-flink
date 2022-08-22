import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment, TableResult}

object FlinkTableMain {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      //.inBatchMode()
      .build()
    val env = TableEnvironment.create(settings)
    val  executeSql = """
        CREATE TABLE TEST_01 (
              key STRING,
              cnt BIGINT,
              ti TIMESTAMP(3),
              WATERMARK FOR ti AS ti - INTERVAL '5' SECOND
            ) WITH (
               'connector' = 'jdbc',
               'url' = 'jdbc:oracle:thin:@192.168.122.128:1521/helowin',
               'table-name' = 'TEST_01',
               'driver'='oracle.jdbc.driver.OracleDriver',
               'username'='orcl',
               'password'='123456'
               );
        """
     val table: TableResult =env.executeSql(executeSql)
   // val querySql="select key,sum(cnt) as cnt from TEST_01 group by key"
   val querySql="""
                select key,sum(cnt) as cnt from TABLE( TUMBLE(TABLE TEST_01, DESCRIPTOR(ti), INTERVAL '1' SECOND))
               GROUP BY key,window_start, window_end
               """
     val rs: Table =env.sqlQuery(querySql)
    val  outSql = """
        CREATE TABLE out_print (
              key STRING,
              cnt BIGINT
            ) WITH (
               'connector' = 'print');
        """
     env.executeSql(outSql)
    rs.executeInsert("out_print")
  }

}
