import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by jianl018 on 2018-12-13.
 */
public class DataDividerByUser {

  public static class DataDividerMapper extends Mapper<Object, Text, Text, Text>{

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      String[] userRating = value.toString().trim().split(",");
      if(userRating.length != 3){
        return;
      }

      context.write(new Text(userRating[0]), new Text(userRating[1] + ":" + userRating[2]));
    }
  }


  public static class DataDividerReducer extends Reducer<Text, Text, Text, Text>{

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      StringBuilder ratings = new StringBuilder();
      for(Text value : values){
        ratings.append("," + value.toString());
      }
      context.write(key, new Text(ratings.toString().replaceFirst(",", "").trim()));
    }
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "DataDividerByUser");
    job.setJarByClass(DataDividerByUser.class);

    job.setMapperClass(DataDividerMapper.class);
    job.setReducerClass(DataDividerReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    TextInputFormat.setInputPaths(job, new Path(args[0]));
    TextOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }
}
