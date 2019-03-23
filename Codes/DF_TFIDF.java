import java.io.IOException;

import java.util.*;

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

public class DF_TFIDF {

    public static class DF_TFIDFMapper extends Mapper<LongWritable, Text, Text, Text>{

        private double page = 0.0;

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{

            String[] val = value.toString().split("\t");
            String[] str;

            if(val[0].indexOf('#') < 0){
                page = Double.parseDouble(val[1]);
            }
            else{
                while (page == 0.0);
                str = val[0].split("#");
                context.write(new Text(str[1] + "#" + String.valueOf(page)), new Text(str[0] + "#" + val[1]));
            }


        }
    }

    public static class DF_TFIDFReducer extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{

            String[] val;
            String[] str = key.toString().split("#");
            double page = Double.parseDouble(str[1]);
            List<String> list = new ArrayList<String>();
            double TF = 0.0;
            double DF = 0.0;
            double TF_IDF = 0.0;

            for(Text value: values){
                list.add(value.toString());
                DF += 1.0;
            }

            for(String item: list){
                val = item.split("#");
                TF = Double.parseDouble(val[1]);
                TF_IDF = TF * Math.log(page / DF + 1.0);
                context.write(new Text(str[0]), new Text(val[0] + "#" + String.valueOf(DF) + "#" + String.valueOf(TF_IDF)));
            }

        }
    }

    public static void main(String args[]) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DF_TFIDF");
        job.setJarByClass(DF_TFIDF.class);

        job.setMapperClass(DF_TFIDFMapper.class);
        job.setReducerClass(DF_TFIDFReducer.class);

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
