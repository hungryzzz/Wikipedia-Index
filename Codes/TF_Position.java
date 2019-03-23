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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TF_Position {

    public static class TF_PositionMapper extends Mapper<LongWritable, Text, Text, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{

            String page = value.toString();

            String[] paragraph;
            String[] words;
            int psum = 0;
            int wsum = 0;

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
                for(int j = 0; j < wsum; j++){
                    if(words[j].length() <= 1 || words[j].indexOf(" ") > 0) continue;
                    context.write(new Text(words[j] + "#" + id), new Text(String.valueOf(i + 1)));
                }
            }
        }
    }

    public static class TF_PositionReducer extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{

            Set<String> set = new HashSet<String>();
            StringBuilder val = new StringBuilder("");
            String str = new String();
            int temp = 0;
            int flag = 1;

            for(Text value: values){
                temp++;
                str = value.toString();
                if(!set.contains(str)){
                    set.add(str);
                    if(flag == 1){
                        val.append(str);
                        flag = 0;
                    }
                    else val.append("-" + str);
                }

            }
            context.write(key, new Text(String.valueOf(temp) + "#" + val.toString()));
        }
    }

    public static void main(String args[]) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TF-Position");
        job.setJarByClass(TF_Position.class);

        job.setMapperClass(TF_PositionMapper.class);
        job.setReducerClass(TF_PositionReducer.class);

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


