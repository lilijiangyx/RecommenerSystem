import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jianl018 on 2018-12-14.
 */
public class Multiplication {
  public static class RelationMapper extends Mapper<LongWritable, Text, Text,Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String itemB = value.toString().split("\t")[0];
      String itemAOccurence = value.toString().split("\t")[1];

      context.write(new Text(itemB), new Text(itemAOccurence));
    }
  }

  public static class UserRatingMapper extends Mapper<LongWritable, Text, Text,Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String userId = value.toString().trim().split(",")[0];
      String itemId = value.toString().trim().split(",")[1];
      String rating = value.toString().trim().split(",")[2];


      context.write(new Text(itemId), new Text(userId + "=" + rating));
    }
  }

  public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Map<String, Double> itemMap = new HashMap<>();
      Map<String, Double> userMap = new HashMap<>();

      String userId;
      double rating;
      String itemId;
      double nOccurence;
      for(Text value : values){
        if(value.toString().contains("=")){
          userId = value.toString().split("=")[0];
          rating = Double.parseDouble(value.toString().split("=")[1]);
          userMap.put(userId, rating);
        }else if(value.toString().contains(":")){
          itemId = value.toString().split(":")[0];
          nOccurence = Double.parseDouble(value.toString().split(":")[1]);
          itemMap.put(itemId, nOccurence);
        }
      }

      for (String user : userMap.keySet()){
        for(String item : itemMap.keySet()){
          double multi = userMap.get(user) * itemMap.get(item);
          context.write(new Text(user + ":" + item), new DoubleWritable(multi));
        }
      }
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf);
    job.setJarByClass(Multiplication.class);

    ChainMapper.addMapper(job, RelationMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
    ChainMapper.addMapper(job, UserRatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

    job.setMapperClass(RelationMapper.class);
    job.setMapperClass(UserRatingMapper.class);

    job.setReducerClass(MultiplicationReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RelationMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, UserRatingMapper.class);

    TextOutputFormat.setOutputPath(job, new Path(args[2]));

    job.waitForCompletion(true);
  }
}
