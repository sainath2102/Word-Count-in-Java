
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
import java.util.StringTokenizer;

public class WordCount {

    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{

        private Text word = new Text();
        private final IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer str = new StringTokenizer(value.toString());
            while(str.hasMoreTokens()){
                word.set(str.nextToken());
                context.write(word,one);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for (IntWritable val : values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        if(args.length !=2){
            System.err.println("Two Input arguments are required <classname(WordCount) input_path output_path> ");
            System.exit(2);
        }

        Job myjob = new Job(conf, "Word Count");
        myjob.setJarByClass(WordCount.class);
        myjob.setMapperClass(WordCountMapper.class);
        myjob.setReducerClass(WordCountReducer.class);

        myjob.setOutputKeyClass(Text.class);
        myjob.setOutputValueClass(IntWritable.class);

        myjob.setNumReduceTasks(3);

        FileInputFormat.addInputPath(myjob,new Path(args[0]));
        FileOutputFormat.setOutputPath(myjob, new Path(args[1]));

        myjob.waitForCompletion(true);
    }
}
