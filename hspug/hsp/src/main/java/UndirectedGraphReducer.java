import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer local que genera aristas bidireccionales dentro de su rango.
 * Cada reducer asume independencia del resto (pueden quedar aristas duplicadas en los bordes).
 */
public class UndirectedGraphReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

    @Override
    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        int N = conf.getInt("num.nodes", 1000);
        long source = key.get();

        // grado local
        long degree = 0;
        for (LongWritable v : values) degree += v.get();

        // rango local aproximado para este reducer (cada uno tiene subconjunto)
        int numReducers = conf.getInt("mapred.reduce.tasks", 1);
        long blockSize = (N + numReducers - 1) / numReducers;
        long reducerId = context.getTaskAttemptID().getTaskID().getId() % numReducers;
        long start = reducerId * blockSize;
        long end = Math.min(N, start + blockSize);

        // target inicial: siguiente nodo en el bloque
        long target = source + 1;
        long created = 0;

        while (created < degree) {
            if (target >= end) target = start; // wrap dentro del bloque
            if (target == source) {
                target++;
                if (target >= end) target = start;
                continue;
            }

            // arista ida
            context.write(new LongWritable(source), new LongWritable(target));
            // arista vuelta
            //context.write(new LongWritable(target), new LongWritable(source));

            created++;
            target++;
        }
    }
}
