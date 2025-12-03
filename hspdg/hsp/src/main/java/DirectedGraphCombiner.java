import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * El Combiner suma los grados de los nodos localmente para optimizar el trabajo.
 */
public class DirectedGraphCombiner extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

    @Override
    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        context.write(key, new LongWritable(sum));
    }
}
