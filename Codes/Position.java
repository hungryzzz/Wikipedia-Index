import java.io.IOException;

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Position {

    public static class PositionMapper extends Mapper<LongWritable, Text, Text, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{

            String page = value.toString();

            String[] paragraph;
            String[] words;
            int psum = 0;
            int wsum = 0;
            Set<String> set = new HashSet<String>();

            String text = TF_DF.TF_DFMapper.get_text(page, "<text", "</text>").toLowerCase();
            StringBuilder strbuild = new StringBuilder(text);
            text = null;
            int len = strbuild.length();
            for(int i = 0; i < len; i++){
                if((strbuild.charAt(i) > 'z' || strbuild.charAt(i) < 'a') && strbuild.charAt(i) != '\n')
                    strbuild.setCharAt(i, ' ');
            }
            paragraph = strbuild.toString().trim().replaceAll(" {2,}", " ").replaceAll("\n{2,}", "\n").split("\n");
            psum = paragraph.length;

            strbuild = null;

            String id = TF_DF.TF_DFMapper.get_text(page, "<id>", "</id>");

            for(int i = 0; i < psum; i++){
                words = paragraph[i].split(" ");
                wsum = words.length;
                set.clear();
                for(int j = 0; j < wsum; j++){
                    if(words[j].length() <= 1 || words[j].indexOf(" ") > 0) continue;
                    if(!set.contains(words[j])){
                        set.add(words[j]);
                        context.write(new Text(words[j] + "#" + id), new Text(String.valueOf(i + 1)));
                    }
                }
            }
        }
    }

    public static class PositionReducer extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{

            String str = new String("");
            int tag = 1;

            for(Text value: values){
                if(tag == 1){
                    tag = 0;
                    str += value.toString();
                }
                else str += "-" + value.toString();

            }

            context.write(key, new Text(str));

        }
    }

    public static void main(String args[]) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Position");
        job.setJarByClass(Position.class);

        job.setMapperClass(PositionMapper.class);
        job.setCombinerClass(PositionReducer.class);
        job.setReducerClass(PositionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
