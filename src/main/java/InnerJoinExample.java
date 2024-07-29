import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

@SuppressWarnings("serial")
public class InnerJoinExample {
    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        MapFunction aa = new MapFunction<String, Tuple2<Integer, String>>()                                     //presonSet = tuple of (1  John)
        {
            public Tuple2<Integer, String> map(String value) {
                String[] words = value.split(",");                                                 // words = [ {1} {John}]
                return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
            }
        };

        DataSet<String> text = env.readTextFile(params.get("input1"));

      //  DataSet<Tuple2<Integer, String>> personSet = env.readTextFile(params.get("input1")).map(xx);

        DataSet<Tuple2<Integer, String>> personSet = text.map(aa);

        System.out.println("------------personSet-updated-------------");
        for (Tuple2<Integer, String> ite : personSet.collect()) {
            System.out.println(ite.toString());
        }
        DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile(params.get("input2")).map(aa);

        // join datasets on person_id
        DataSet<Tuple3<Integer, String, String>> joined = personSet.join(locationSet).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {

                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer, String> location) {
                        return new Tuple3<Integer, String, String>(person.f0, person.f1, location.f1);         // returns tuple of (1 John DC)
                    }
                });

        joined.writeAsCsv(params.get("output"), "\n", " ");

        env.execute("Inner Join example for local fink");
    }
}
