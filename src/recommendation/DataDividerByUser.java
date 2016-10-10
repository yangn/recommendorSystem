package recommendation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import utils.MapReduceHelper;

import java.io.IOException;
import java.lang.InterruptedException;

public class DataDividerByUser {

    public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input format: userId,movieId,rating
            String[] userId_movieId_rating = value.toString().trim().split(",");
            int userId = Integer.parseInt(userId_movieId_rating[0]);
            String movieId = userId_movieId_rating[1];

            context.write(new IntWritable(userId), new Text(movieId));
        }
    }

    public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable userId, Iterable<Text> movieIds, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            while (movieIds.iterator().hasNext()) {
                sb.append(movieIds.iterator().next() + ",");
            }
            sb.deleteCharAt(sb.length() - 1);
            String movieRatings = sb.toString();

            //key=userId value=move1:rating, movie2:rating
            context.write(userId, new Text(movieRatings));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 2) {
            System.out.println("Requires two input argument: inputPath, outputPath.");
            throw new IllegalArgumentException("Requires two input argument: inputPath, outputPath.");
        }
        String inputPath = args[0];
        String outputPath = args[1];

        Configuration conf = new Configuration();
        MapReduceHelper.deleteDirectory(FileSystem.get(conf), new Path(outputPath));
        Job job = Job.getInstance(conf);

        job.setMapperClass(DataDividerMapper.class);
        job.setReducerClass(DataDividerReducer.class);

        job.setJarByClass(DataDividerByUser.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(inputPath));
        TextOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
