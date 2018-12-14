import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by jianl018 on 2018-12-14.
 */
public class CoOccurenceMatrixGenerator {

  public static class CoOccurenceMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] itemsRatings = value.toString().trim().split("\t")[1].split(",");

      for(int i = 0; i < itemsRatings.length; i++){
        String itemIdA = itemsRatings[i].split(":")[0];

        for(int j = 0; j < itemsRatings.length; j++){
          String itemIdB = itemsRatings[j].split(":")[0];
          context.write(new Text(itemIdA + ":" + itemIdB), new IntWritable(1));
        }
      }
    }
  }

  public static class CoOccurenceReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      while(values.iterator().hasNext()){
        sum += values.iterator().next().get();
      }
      context.write(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "CoOccurenceMatrixGenerator");
    job.setJarByClass(CoOccurenceMatrixGenerator.class);

    job.setMapperClass(CoOccurenceMapper.class);
    job.setReducerClass(CoOccurenceReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    TextInputFormat.setInputPaths(job, new Path(args[0]));
    TextOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }
}
