import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Versión paralela: cada mapper reduce los grados de sus targets locales
 * antes de emitir (nodo, grado). Acepta pequeños errores en los bordes.
 */
public class UndirectedGraphMapper extends Mapper<NullWritable, NullWritable, LongWritable, LongWritable> {

    private int[] degrees;
    private int numNodes;
    private int numMappers;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        this.numNodes = conf.getInt("num.nodes", 1000);
        this.numMappers = conf.getInt("num.mappers", 1);
        this.degrees = new int[numNodes];

        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles == null || cacheFiles.length == 0)
            throw new IOException("Archivo degrees.dat no encontrado en caché");

        FileSystem fs = FileSystem.get(cacheFiles[0], conf);
        try (DataInputStream dis = new DataInputStream(fs.open(new Path(cacheFiles[0])))) {
            int n = dis.readInt();
            if (n != numNodes) throw new IOException("num.nodes no coincide con archivo");
            for (int i = 0; i < numNodes; i++) degrees[i] = dis.readInt();
        }
    }

    @Override
    protected void map(NullWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        int taskId = context.getTaskAttemptID().getTaskID().getId();
        int nodesPerMapper = numNodes / numMappers;
        int start = taskId * nodesPerMapper;
        int end = (taskId == numMappers - 1) ? numNodes : start + nodesPerMapper;

        // Copia local de los grados en este rango
        int[] local = new int[end - start];
        System.arraycopy(degrees, start, local, 0, end - start);

        // Reducción local: cada nodo "consume" 1 grado de sus siguientes targets dentro del rango
        for (int i = 0; i < local.length; i++) {
            int d = local[i];
            int target = i + 1;
            while (d > 0 && target < local.length) {
                if (local[target] > 0) local[target]--; // resta en el target
                d--;
                target++;
            }
        }

        // Emitir (nodo, grado reducido)
        for (int i = 0; i < local.length; i++) {
            if (local[i] > 0) {
                context.write(new LongWritable(start + i), new LongWritable(local[i]));
            }
        }
    }
}
