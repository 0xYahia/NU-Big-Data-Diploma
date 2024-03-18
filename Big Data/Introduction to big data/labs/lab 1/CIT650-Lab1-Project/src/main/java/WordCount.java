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

public class WordCount {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2){
            System.out.println("Usage WordCount <input-file> <output-dir>");
        }
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, "Word Count - CIT650");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }

    public static class MapperClass extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            // key: line id
            // value: the line itself
            // context: to send the intermediate results

            // Splitting the line by space using StringTokenizer
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            // Iterating over the list of words for the line and emitting a tuple of (word, 1)
            while (stringTokenizer.hasMoreTokens()){
                word.set(stringTokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable>{

        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

            // key: word
            // values: list of ones
            // context: to send the final results

            // Container to sum the ones of the same key
            int sum = 0;
            // Summing the ones from the list of values
            for (IntWritable intWritable : values){
                sum += intWritable.get();
            }
            // Setting the sum result to IntWritable
            result.set(sum);
            // Emitting the final result as a tuple of (word, count)
            context.write(key, result);

        }
    }
}
