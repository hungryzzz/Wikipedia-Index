import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TF_DF {

    public static class TF_DFMapper extends Mapper<LongWritable, Text, Text, Text>{

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
                context.write(new Text(word[i] + "#" + id), new Text(String.valueOf(sum)));
            }

        }
    }

    public static class TF_DFCombiner extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{

            int sum = 1;
            double temp = 0.0;
            for(Text value: values){
                sum = Integer.parseInt(value.toString());
                temp += 1.0;
            }

            String[] val = key.toString().split("#");
            context.write(new Text(val[0]), new Text(val[1] + "#" + String.valueOf(temp / (sum * 1.0))));

        }
    }

    public static class TF_DFReducer extends Reducer<Text, Text, Text, MyWritable>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{

            double temp = 0.0;
            List<String> list = new ArrayList<String>();

            for(Text value: values){
                temp += 1.0;
                list.add(value.toString());
            }

            MyWritable out = new MyWritable(String.valueOf(temp), list);

            context.write(new Text("{\"" + key + "\":"), out);

        }
    }

    public static void main(String [] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TF_DF");
        job.setJarByClass(TF_DF.class);

        job.setMapperClass(TF_DFMapper.class);
        job.setCombinerClass(TF_DFCombiner.class);
        job.setReducerClass(TF_DFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyWritable.class);

        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
