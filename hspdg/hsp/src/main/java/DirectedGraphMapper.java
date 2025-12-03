import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * El Mapper lee el vector de grados de la caché distribuida y emite pares (nodo, grado)
 * para los nodos que le corresponden.
 */
public class DirectedGraphMapper extends Mapper<NullWritable, NullWritable, LongWritable, LongWritable> {

    private int[] degrees;
    private int numNodes;
    private int numMappers;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.numNodes = conf.getInt("num.nodes", 1000);
        this.numMappers = conf.getInt("num.mappers", 1);
        this.degrees = new int[this.numNodes];

        // Obtiene la URI del archivo de caché y lee el vector de grados
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            URI degreesUri = cacheFiles[0];
            FileSystem fs = FileSystem.get(degreesUri, conf);
            Path degreesPath = new Path(degreesUri);
            
            try (DataInputStream dis = new DataInputStream(fs.open(degreesPath))) {
                int nodesCount = dis.readInt();
                if (nodesCount != this.numNodes) {
                    throw new IOException("El número de nodos en el archivo no coincide con la configuración.");
                }
                for (int i = 0; i < this.numNodes; i++) {
                    this.degrees[i] = dis.readInt();
                }
            }
        }
    }

    @Override
    public void map(NullWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        // La tarea actual necesita saber su ID para determinar su rango de trabajo.
        int taskId = context.getTaskAttemptID().getTaskID().getId();
        
        // Calcula el rango de nodos para este Mapper
        int nodesPerMapper = numNodes / numMappers;
        int startNode = taskId * nodesPerMapper;
        int endNode = (taskId == numMappers - 1) ? numNodes : startNode + nodesPerMapper;

        // Itera sobre el rango asignado y emite los pares (nodo, grado)
        for (int i = startNode; i < endNode; i++) {
            if (degrees[i] > 0) {
                context.write(new LongWritable(i), new LongWritable(degrees[i]));
            }
        }
    }
}