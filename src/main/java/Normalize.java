import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jianl018 on 2018-12-14.
 */
public class Normalize {

  public static class NormalizeMapper extends Mapper<LongWritable, Text, Text,Text>{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String itemAB = value.toString().trim().split("\t")[0];
      String occurence = value.toString().split("\t")[1];

      String itemA = itemAB.split(":")[0];
      String itemB = itemAB.split(":")[1];

      context.write(new Text(itemA), new Text(itemB + ":" + occurence));
    }
  }

  public static class NormalizerReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      double sum = 0;
      Map<String, Integer> map = new HashMap<>();

      for(Text value : values){
        String itemB = value.toString().split(":")[0];
        Integer occurence = Integer.parseInt(value.toString().split(":")[1]);
        sum += occurence;
        map.put(itemB, occurence);
      }

      for(String mapKey : map.keySet()){
        String itemB = mapKey;
        double normalizedOccurence = (double) map.get(mapKey) / sum;
        context.write(new Text(itemB), new Text(key.toString() + ":" + normalizedOccurence));
      }
    }
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Normalize");
    job.setJarByClass(Normalize.class);

    job.setMapperClass(NormalizeMapper.class);
    job.setReducerClass(NormalizerReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    TextInputFormat.setInputPaths(job, new Path(args[0]));
    TextOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }
}
