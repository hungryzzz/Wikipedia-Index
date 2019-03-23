import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.rmi.PortableRemoteObject;

public class TF {

    public static class TFMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        public static String get_text(String page, String start_key, String end_key){
            int start = page.indexOf(start_key, 0);
            if(start < 0) return "";
            int end = page.indexOf(end_key, start + 1);
            if(end < 0) return "";
            while(page.charAt(start) != '>') start++;
            return page.substring(start + 1, end);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{

            String page = value.toString();

            String[] word;
            int sum = 0;

            String text = get_text(page, "<text", "</text>").toLowerCase();
            StringBuilder strbuild = new StringBuilder(text);
            text = null;
            int len = strbuild.length();
            for(int i = 0; i < len; i++){
                if(strbuild.charAt(i) > 'z' || strbuild.charAt(i) < 'a')
                    strbuild.setCharAt(i, ' ');
            }
            word = strbuild.toString().trim().replaceAll(" {2,}", " ").split(" ");
            if(word.length > 0) sum = word.length;
            strbuild = null;

            String id = get_text(page, "<id>", "</id>");

            for(int i = 0; i < sum; i++){
                if(word[i].length() == 1) continue;
                context.write(new Text(word[i] + "#" + id), new IntWritable(sum));
            }
        }
    }

    public static class TFReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException{

            int sum = 1;
            int temp = 0;

            for(IntWritable value: values){
                temp++;
                sum = value.get();
            }

            context.write(key, new DoubleWritable((temp * 1.0) / (sum * 1.0)));
        }
    }

    public static void main(String args[]) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Term Frequency");
        job.setJarByClass(TF.class);

        job.setMapperClass(TFMapper.class);
        job.setReducerClass(TFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
