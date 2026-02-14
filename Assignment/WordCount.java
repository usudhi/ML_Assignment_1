import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    /**
     * Mapper Class.

     */
    public static class TokenizerMapper 
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            // Clean the input: Remove punctuation and convert to lowercase
            // Regex [^a-zA-Z0-9 ] removes everything except alphanumeric characters and spaces
            String cleanLine = value.toString().replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase();
            
            StringTokenizer itr = new StringTokenizer(cleanLine);
            
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    /**
     * Reducer Class
     */
    public static class IntSumReducer 
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * Driver Method
     */
    public static void main(String[] args) throws Exception {
        // Capture start time for performance measurement
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        

        conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 134217728L);

        Job job = Job.getInstance(conf, "Timed Word Count");
        
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        

        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        if (args.length < 2) {
            System.err.println("Usage: WordCount <in> <out>");
            System.exit(2);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        boolean success = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println("========================================");
        System.out.println("TOTAL JOB EXECUTION TIME: " + (endTime - startTime) + " ms");
        System.out.println("========================================");

        System.exit(success ? 0 : 1);
    }
}