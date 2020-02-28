package edu.gatech.cse6242;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Q4 
{
public static class Map extends Mapper<Object, Text, Text, IntWritable>
    {
       private Text num1 = new Text();
      private Text num2 = new Text();
      private final static IntWritable in = new IntWritable(1);
      private final static IntWritable out= new IntWritable(-1);
    
      // map function is for --Mapper--, it process line by line
      public void map(Object key, Text value, Context context) 
      throws IOException, InterruptedException 
        {  
       String[] content = value.toString().split("\t");
          num1.set(content[0]);
          num2.set(content[1]);
           context.write(num1,in);
          context.write(num2, out);
        }
    }

  public static class SReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
  {
      private IntWritable result = new IntWritable();
      public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException 
      {
        int sum = 0;
        for (IntWritable val : values) 
        {
          sum = sum+val.get();
        }
        result.set(sum);
        context.write(key, result);
      }
  }


  // ----------------------------- Reduce 2 -----------------------------------

  public static class Map2 extends Mapper<Object, Text, Text, IntWritable>
    {

      private final static IntWritable in = new IntWritable(1);
      private Text num3 = new Text();
     
      public void map(Object key, Text value, Context context) 
      throws IOException, InterruptedException 
        {  
          String [] content = value.toString().split("\t");
                num3.set(content[1]);
                context.write(num3, in);
        }
    }

    public static class SReducer2 extends Reducer<Text,IntWritable,Text,IntWritable> 
  {
      private IntWritable result = new IntWritable();

      public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException 
      {
        int sum = 0;
        for (IntWritable val : values) 
        {
          sum = sum+val.get();
        }
        result.set(sum);
        context.write(key, result);
      }
  }





  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "FIRST");

    /* TODO: Needs to be implemented */
    job.setJarByClass(Q4.class);
    job.setMapperClass(Map.class);
 //   job.setCombinerClass(SReducer.class);
    job.setReducerClass(SReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("tmp"));


    job.waitForCompletion(true);
    Job job2 = Job.getInstance(conf, "SECOND");
    job2.setJarByClass(Q4.class);
    job2.setMapperClass(Map2.class);
    //job2.setCombinerClass(SReducer2.class);
    job2.setReducerClass(SReducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);


    FileInputFormat.addInputPath(job2, new Path("tmp"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    //FileSystem.delete(Path "tmp", boolean recursive);

    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}