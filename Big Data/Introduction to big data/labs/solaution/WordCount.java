import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class WordCount {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text city = new Text();
        private final IntWritable temperature = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] tokens = value.toString().split(",");
            if (tokens.length >= 2) {
                city.set(tokens[0]);
                try {
                    double temp = Double.parseDouble(tokens[1]);
                    temperature.set((int) temp);
                    context.write(city, temperature);
                } catch (NumberFormatException e) {
                    // Ignore invalid temperature values
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, Text> {

        private final Text avgTemperatureText = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            int count = 0;

            for (IntWritable value : values) {
                sum += value.get();
                count++;
            }

            if (count > 0) {
                double avg = (double) sum / count;
                String avgWithSymbol = String.format("%.1f℃", avg);
                avgTemperatureText.set(avgWithSymbol);
                context.write(key, avgTemperatureText);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: CityTemperatureAverage <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "City Temperature Average");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}