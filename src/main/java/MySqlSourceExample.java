import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;

public class MySqlSourceExample {
  public static void main(String[] args) throws Exception {
    MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("127.0.0.1")
            .port(3306)
            .databaseList("flink_test") // set captured database
            .tableList("flink_test.products") // set captured table
            .username("root")
            .password("root")
            .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
            .build();

    Configuration configuration=new Configuration();
    configuration.setInteger(RestOptions.PORT, 8081);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

    // enable checkpoint
    env.enableCheckpointing(3000);

    env
      .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
            .addSink(new CustomeSink())
      // set 4 parallel source tasks
      .setParallelism(4);
//            ..print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

    env.execute("Print MySQL Snapshot + Binlog");
  }
}