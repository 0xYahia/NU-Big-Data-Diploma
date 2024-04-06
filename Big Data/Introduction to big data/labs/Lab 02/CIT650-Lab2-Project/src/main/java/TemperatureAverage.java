package main.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class TemperatureAverage {

    // Mapper class processes input data and produces key-value pairs
    public static class MapperClass extends Mapper<Object, Text, Text, IntWritable> {

        private final Text city = new Text();
        private final IntWritable temperature = new IntWritable();

        // The map method is called for every line in the input data
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Split the input line into tokens using comma as the separator
            String[] tokens = value.toString().split(",");
            // Check if the line has at least two tokens (city and temperature)
            if (tokens.length >= 2) {
                city.set(tokens[0]); // Set the city name as the key
                try {
                    // Parse the temperature and write the key-value pair to the context
                    int temp = (int) Double.parseDouble(tokens[1]);
                    temperature.set(temp);
                    context.write(city, temperature);
                } catch (NumberFormatException e) {
                    // Log invalid temperature values and continue
                    context.getCounter("Temperature", "Invalid").increment(1);
                }
            }
        }
    }

    // Reducer class aggregates the values associated with the same key
    public static class ReducerClass extends Reducer<Text, IntWritable, Text, Text> {

        private final Text avgTemperatureText = new Text();

        // The reduce method is called for each key with its associated values
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // Calculate the sum of temperatures using Java 8 streams
            double sum = StreamSupport.stream(values.spliterator(), false)
                    .mapToInt(IntWritable::get)
                    .summaryStatistics()
                    .getSum();

            // Count the number of temperature readings
            long count = StreamSupport.stream(values.spliterator(), false).count();

            // Calculate the average temperature if there are readings
            if (count > 0) {
                double avg = sum / count;
                // Format the average temperature to one decimal place with the Celsius symbol
                String avgWithSymbol = String.format("%.1f℃", avg);
                avgTemperatureText.set(avgWithSymbol);
                // Write the city and its average temperature to the context
                context.write(key, avgTemperatureText);
            }
        }
    }

    // Main method configures and runs the MapReduce job
    public static void main(String[] args) throws Exception {
        // Check for the correct number of arguments
        if (args.length != 2) {
            System.err.println("Usage: CityTemperatureAverage <input path> <output path>");
            System.exit(-1);
        }

        // Set up the configuration and job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "City Temperature Average");

        // Specify the jar file that contains the driver, mapper, and reducer classes
        job.setJarByClass(TemperatureAverage.class);
        // Set the mapper and reducer classes
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        // Set the output types for the mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set the output types for the reducer (final output)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output paths from the job arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exit with the appropriate job completion status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
