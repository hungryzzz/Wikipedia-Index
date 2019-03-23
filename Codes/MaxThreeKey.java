import java.io.IOException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MaxThreeKey {

    public static class MaxThreeKeyMapper extends Mapper<LongWritable, Text, Text, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{

            String[] val = value.toString().split("\t");
            String[] str = val[1].split("#");
            context.write(new Text(str[0]), new Text(val[0] + "#" + str[2]));

        }
    }

    public static class MaxThreeKeyReducer extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{

            double max1, max2, max3;
            max1 = max2 = max3 = Double.MIN_VALUE;
            String word1 = new String();
            String word2 = new String();
            String word3 = new String();
            double temp = 0;
            String[] str;
            for(Text value: values){
                str = value.toString().split("#");
                temp = Double.parseDouble(str[1]);
                if(temp > max1){
                    max3 = max2; max2 = max1; max1 = temp;
                    word3 = word2; word2 = word1; word1 = str[0];
                }
                else if(temp > max2){
                    max3 = max2; max2 = temp;
                    word3 = word2; word2 = str[0];
                }
                else if(temp > max3){
                    max3 = temp;
                    word3 = str[0];
                }
            }
            context.write(new Text(word1), new Text(key + "#" + max1));
            context.write(new Text(word2), new Text(key + "#" + max2));
            context.write(new Text(word3), new Text(key + "#" + max3));

        }
    }

    public static void main(String args[]) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MaxThreeKey");
        job.setJarByClass(MaxThreeKey.class);

        job.setMapperClass(MaxThreeKeyMapper.class);
        job.setReducerClass(MaxThreeKeyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
