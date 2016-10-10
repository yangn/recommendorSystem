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

public class CoOccurrenceMatrixGenerator {

    public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //value = userId\t movieId1, movieId2...
            String line = value.toString().trim();
            String[] userId_movieIds = line.split("\t");
            String userId = userId_movieIds[0];
            String[] movieIds = userId_movieIds[1].split(",");
            for (int i = 0; i < movieIds.length; i++) {
                String movieId = movieIds[i];
                for (int j = 0; j < movieIds.length; j++) {
                    String anotherMovieId = movieIds[j];
                    //key = movie1: movie2 value = 1
                    context.write(new Text(movieId + ":" + anotherMovieId), new IntWritable(1));
                }
            }
        }
    }

    public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text movieIdPair, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //movieIdPair movie1:movie2 value= iterable<1,1,1,...>
            int sum = 0;
            while (values.iterator().hasNext()) {
                sum += values.iterator().next().get();
            }
            context.write(movieIdPair, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args == null || args.length < 2) {
            System.out.println("Requires two input argument: inputPath, outputPath.");
            throw new IllegalAccessException("Requires two input argument: inputPath, outputPath.");
        }
        String inputPath = args[0];
        String outputPath = args[1];

        Configuration conf = new Configuration();
        MapReduceHelper.deleteDirectory(FileSystem.get(conf), new Path(outputPath));
        Job job = Job.getInstance(conf);

        job.setMapperClass(MatrixGeneratorMapper.class);
        job.setReducerClass(MatrixGeneratorReducer.class);

        job.setJarByClass(CoOccurrenceMatrixGenerator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job, new Path(inputPath));
        TextOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
