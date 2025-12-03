import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.io.DataOutput;
import java.io.DataInput;
import org.apache.hadoop.io.Writable;

public class EmptyInputFormat extends InputFormat<NullWritable, NullWritable> {

    /**
     * Este método crea un número de splits igual al número de mappers deseado,
     * sin necesidad de archivos de entrada.
     */
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int numMappers = conf.getInt("num.mappers", 1);
        
        List<InputSplit> splits = new ArrayList<>(numMappers);
        for (int i = 0; i < numMappers; i++) {
            splits.add(new EmptySplit());
        }
        return splits;
    }

    /**
     * Este método retorna un RecordReader que no lee ningún dato,
     * ya que los datos se distribuyen por separado.
     */
    @Override
    public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new EmptyRecordReader();
    }

    /**
     * Un split vacío para representar una tarea de mapper sin datos de entrada.
     */
    public static class EmptySplit extends InputSplit implements org.apache.hadoop.io.Writable {
        @Override
        public long getLength() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }

        @Override
        public void write(DataOutput out) throws IOException {
            // No hay datos para escribir
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            // No hay datos para leer
        }
    }
    
    /**
     * Un RecordReader que siempre retorna falso, indicando que no hay más registros.
     */
    public static class EmptyRecordReader extends RecordReader<NullWritable, NullWritable> {
        private boolean hasNext = true;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            // No se necesita inicializar nada
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (hasNext) {
                hasNext = false;
                return true;
            }
            return false;
        }

        @Override
        public NullWritable getCurrentKey() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public NullWritable getCurrentValue() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return hasNext ? 0.0f : 1.0f;
        }

        @Override
        public void close() throws IOException {
            // No se necesita cerrar nada
        }
    }
}
