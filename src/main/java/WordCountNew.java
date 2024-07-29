import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCountNew {
    public static void main(String[] args)
            throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = env.readTextFile(params.get("input"));

        FilterFunction<String> ooo= new FilterFunction<String>() {
            public boolean filter(String value) {
                return value.startsWith("N");
            }
        };

        DataSet<String> filtered = text.filter(ooo);

        System.out.println("------------filtered--------------");
        for (String item : filtered.collect()) {
            System.out.println(item);
        }


        Tokenizer tokenizer = new Tokenizer();
        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(tokenizer);

        System.out.println("------------tokenized--------------");
        for (Tuple2<String, Integer> item : tokenized.collect()) {
            System.out.println(item.toString());
        }

        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(new int[]{0}).sum(1);
        System.out.println("------------counts--------------");
        for (Tuple2<String, Integer> ite : counts.collect()) {
            System.out.println(ite.toString());
        }

        if (params.has("output")) {
            counts.writeAsCsv(params.get("output"), "\n", " ");

            env.execute("WordCount Example");
        }
    }

    public static final class Tokenizer   implements MapFunction<String, Tuple2<String, Integer>> {
        public Tuple2<String, Integer> map(String value) {
            return new Tuple2(value, Integer.valueOf(1));
        }
    }
}
