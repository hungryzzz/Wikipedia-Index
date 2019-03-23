import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.rmi.PortableRemoteObject;

public class DF {

    public static class DFMapper extends Mapper<LongWritable, Text, Text, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{

            String[] val = value.toString().split("\t");
            String[] str = val[0].split("#");

            context.write(new Text(str[0]), new Text(val[1] + "#" + str[1]));
        }
    }

    public static class DFReducer extends Reducer<Text, Text, Text, MyWritable>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{

            int sum  = 0;
            List<String> list = new ArrayList<String>();

            for(Text value: values){
                sum++;
                list.add(value.toString());
            }

            MyWritable output = new MyWritable(String.valueOf(sum), list);

            context.write(new Text("{\"" + key + "\":"), output);
        }
    }

    public static void main(String args[]) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TFDF");
        job.setJarByClass(DF.class);

        job.setMapperClass(DFMapper.class);
        job.setReducerClass(DFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
