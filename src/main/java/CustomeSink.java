import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class CustomeSink extends RichSinkFunction<String> {


    //向下游写入
    @Override
    public void invoke(String value, Context context) throws Exception {
        System.out.println(">>>>>>>"+value);
    }


}
