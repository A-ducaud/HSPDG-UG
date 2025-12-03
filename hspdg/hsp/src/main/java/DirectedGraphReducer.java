import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Reducer builds the final edges of the graph in an undirected fashion.
 */
public class DirectedGraphReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

    @Override
    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        long N = conf.getInt("num.nodes", 1000);

        Long source = Long.valueOf(key.toString());
        long degree = 0;

        for (LongWritable val : values) {
            degree += val.get();
        }
        
        // The main logic generates the edges of the undirected graph
        writeEdgeAlwaysGoingForward(degree, source, N, context);
    }

    /**
     * Helper method to write edges, ensuring each edge is only written once.
     * The method works by always creating an edge from a lower index node to a higher index node.
     */
    private void writeEdgeAlwaysGoingForward(long degree, long source, long N, Context context) throws IOException, InterruptedException {
        long i = 0;
        long target = source + 1;
        
        // Generates edges to nodes with a higher index
        while (i < degree && target < N) {
            LongWritable nkey = new LongWritable(source);
            LongWritable nvalue = new LongWritable(target);
            context.write(nkey, nvalue);
            i++;
            target++;
        }
        
        // If needed, generates edges to nodes with a lower index to reach the desired degree
        target = 0;
        while (i < degree) {
            LongWritable nkey = new LongWritable(source);
            LongWritable nvalue = new LongWritable(target);
            context.write(nkey, nvalue);
            i++;
            target++;
        }
    }
}
