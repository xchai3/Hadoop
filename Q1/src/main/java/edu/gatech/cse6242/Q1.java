package edu.gatech.cse6242;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1 {

  public static class Map extends Mapper<Object, Text, Text, Text> {
    private Text node = new Text();
   // private IntWritable weight = new IntWritable();
   private Text target_weight=new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
  String line = value.toString();
        String[] content = line.split("\t");
        node.set(content[0]);
        target_weight.set(content[1]+","+content[2]);
      //  weight.set(Integer.parseInt(content[2]));
        //context.write(node, weight);
        context.write(node,target_weight);
    }
  }
  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int weightMax = 0;
      int target=Integer.MAX_VALUE;
        for (Text val : values) 
        {   
            String line=val.toString();
            String[] temp=line.split(",");
           int temp0=Integer.parseInt(temp[0]);
           int  temp1=Integer.parseInt(temp[1]);
            if(temp1>weightMax){
                weightMax=temp1;
                target=temp0;
            }
            if(temp1==weightMax)
            {
            	target=Math.min(target,temp0);
            }
       //   sum = Math.max(val.get(), sum);
        }
        String ans=target+","+weightMax;
       // System.out.println("String!: "+ans);
        result.set(ans);
        context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q1");

    /* TODO: Needs to be implemented */
    job.setJarByClass(Q1.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(IntSumReducer.class);
    //job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);


    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
