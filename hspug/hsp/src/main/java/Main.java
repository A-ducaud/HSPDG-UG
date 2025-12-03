import java.io.*;
import java.net.URI;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static java.lang.Math.*;

public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Main(), args));
    }

    @Override
    public int run(String[] args) throws Exception {

        // --- Parámetros ---
        int numNodes = 1000;
        int numWorkers = 1;
        boolean argHasW = false;

        for (int i = 0; i < args.length; i++) {
            if ("-n".equals(args[i])) {
                numNodes = Integer.parseInt(args[++i]);
            } else if ("-w".equals(args[i])) {
                numWorkers = Integer.parseInt(args[++i]);
                argHasW = true;
            } else {
                System.err.println("Uso: Main -n <num_nodos> [-w <num_workers>]");
                return 1;
            }
        }

        System.out.println("=== Parámetros ===");
        System.out.println("Nodos: " + numNodes);
        System.out.println("Workers (flag -w): " + numWorkers);

        long startTime = System.nanoTime();

        // --- Generar el vector de grados ---
        System.out.println("Generando vector de grados...");
        int[] degrees = PowerLawGraph.generateDegrees(numNodes);
        System.out.println("Vector generado.");

        long totalEdges = 0;
        for (int d : degrees) totalEdges += d;
        System.out.println("Suma total de grados: " + totalEdges);

        // --- Configuración del Job ---
        Configuration conf = getConf();
        conf.setInt("num.nodes", numNodes);
        conf.setInt("num.workers", numWorkers);

        // Selección automática de mappers
        int numMappers;
        if (numWorkers > 1 && !argHasW) {
            numMappers = (int) max(numWorkers, round(log(numNodes)));
        } else {
            numMappers = numWorkers;
        }
        if (numMappers < 1) numMappers = 1;
        conf.setInt("num.mappers", numMappers);

        System.out.println("Map tasks: " + numMappers);

        Path inputDir = new Path("/graph_input");
        Path outputDir = new Path("/graph_output");

        FileSystem fs = FileSystem.get(conf);

        // Solo borra salida previa si existe
        if (fs.exists(outputDir)) {
            System.out.println("Eliminando salida previa...");
            fs.delete(outputDir, true);
        }

        // --- Guardar el vector de grados en caché distribuida ---
        Path degreesPath = new Path(inputDir, "degrees.dat");
        if (fs.exists(inputDir)) fs.delete(inputDir, true);
        fs.mkdirs(inputDir);

        try (FSDataOutputStream out = fs.create(degreesPath, true);
             BufferedOutputStream bos = new BufferedOutputStream(out);
             DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(numNodes);
            for (int deg : degrees) dos.writeInt(deg);
        }

        // --- Crear Job Hadoop ---
        Job job = Job.getInstance(conf, "Undirected Power Law Graph Generation");
        job.setJarByClass(Main.class);
        job.addCacheFile(degreesPath.toUri());

        // --- Configurar clases ---
        job.setInputFormatClass(EmptyInputFormat.class);
        job.setMapperClass(UndirectedGraphMapper.class);
        job.setCombinerClass(UndirectedGraphCombiner.class);
        job.setReducerClass(UndirectedGraphReducer.class);

        // Reducers: igual al número de workers (más eficiente que 1 solo)
        job.setNumReduceTasks(numWorkers);

        // Tipos de salida intermedia y final
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileOutputFormat.setOutputPath(job, outputDir);

        // --- Ejecutar ---
        boolean success = job.waitForCompletion(true);

        long durationNano = System.nanoTime() - startTime;
        double seconds = durationNano / 1_000_000_000.0;
        System.out.printf("Tiempo total: %.3f segundos%n", seconds);

        // --- Combinar archivos de salida (getmerge por API Hadoop) ---
        if (success) {
            Path mergedFile = new Path("/graph_output_final.txt");
            System.out.println("Uniendo archivos de salida en " + mergedFile + " ...");

            try (FSDataOutputStream out = fs.create(mergedFile, true)) {
                FileStatus[] parts = fs.listStatus(outputDir);
                byte[] buffer = new byte[8192];

                for (FileStatus part : parts) {
                    if (!part.isFile()) continue;
                    String name = part.getPath().getName();
                    if (!name.startsWith("part-")) continue;

                    try (FSDataInputStream in = fs.open(part.getPath())) {
                        int bytes;
                        while ((bytes = in.read(buffer)) > 0) {
                            out.write(buffer, 0, bytes);
                        }
                    }
                }
            }

            System.out.println("Merge completado exitosamente en /graph_output_final.txt");
        }

        return success ? 0 : 1;
    }
}
