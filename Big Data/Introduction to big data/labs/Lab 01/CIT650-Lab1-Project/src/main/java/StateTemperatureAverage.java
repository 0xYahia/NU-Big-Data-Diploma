import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.StringTokenizer;

public class StateTemperatureAverage {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2){
            System.out.println("Usage StateTemperatureAverage <input-file> <output-dir>");
        }
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, "State Temperature Average - CIT650");

        job.setJarByClass(StateTemperatureAverage.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);  //System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class MapperClass extends Mapper<Object, Text, Text, DoubleWritable>{

        private final static DoubleWritable temperature = new DoubleWritable();
        private final Text state = new Text();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException {

            // key: line id
            // value: the line itself
            // context: to send the intermediate results

            // Splitting the input line by comma
            String[] tokens = value.toString().split(",");
            if (tokens.length >= 2) { // Ensuring we have at least state and temperature
                state.set(tokens[0].trim().toLowerCase()); // State
                temperature.set(Double.parseDouble(tokens[1].trim())); // Temperature

                context.write(state, temperature);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

        private DoubleWritable result = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException {

            // key: state
            // values: list of ones
            // context: to send the final results

            int count = 0;
            double sum = 0;

            // Calculate the sum and count of temperatures for each state
            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }

            // Calculate the average temperature for the state
            double average = sum / count;

            // Emit the state and its average temperature
            context.write(key, new DoubleWritable(average));

        }
    }
}
