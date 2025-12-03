import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner: suma grados locales por nodo.
 */
public class UndirectedGraphCombiner extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
    @Override
    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable v : values) sum += v.get();
        context.write(key, new LongWritable(sum));
    }
}
