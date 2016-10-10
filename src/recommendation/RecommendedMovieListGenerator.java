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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import utils.MapReduceHelper;

import java.io.IOException;
import java.util.*;

public class RecommendedMovieListGenerator {

    public static final String WATCHED_TAG = "WATCHED_MOVIE";

    public static final String ESTIMATED_RATING_TAG = "ESTIMATED_MOVIE_RATING";

    public static class WatchedMovieMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        //read user_movie_ratings file and output user:movie:rating for watched movies with tag
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] user_movie_rating = value.toString().trim().split(",");
            String user = user_movie_rating[0];
            String movie = user_movie_rating[1];
            String rating = user_movie_rating[2];

            context.write(new IntWritable(Integer.valueOf(user)), new Text(WATCHED_TAG + ":" + movie + ":" + rating));
        }

    }

    public static class MovieRatingEstimationMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        //read estimated_movie_rating_for_user file, and output user:movie:estimatedRating it with tag
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // user:movie\testimatedScore
            String[] userMovie_estimatedScore = value.toString().trim().split("\t");
            String userMovie = userMovie_estimatedScore[0];
            String estimatedRating = userMovie_estimatedScore[1];
            String[] user_movie = userMovie.split(":");
            String user = user_movie[0];
            String movie = user_movie[1];

            context.write(new IntWritable(Integer.valueOf(user)), new Text(ESTIMATED_RATING_TAG + ":" + movie + ":" + estimatedRating));
        }
    }

    public static class RecommendedMovieListGeneratorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Map<String, Double> watchedMovieRatings = new HashMap<String, Double>();
            Map<String, Double> estimatedMovieRatings = new HashMap<String, Double>();
            double watchedMovieRatingSum = 0d;
            while(values.iterator().hasNext()) {
                Text value = values.iterator().next();
                String[] tag_movie_rating = value.toString().split(":");
                String tag = tag_movie_rating[0];
                String movie = tag_movie_rating[1];
                if (WATCHED_TAG.equals(tag)) {
                    String rating = tag_movie_rating[2];
                    watchedMovieRatings.put(movie, Double.valueOf(rating));
                    watchedMovieRatingSum += Double.valueOf(rating);
                } else if (ESTIMATED_RATING_TAG.equals(tag)) {
                    String estimatedRating = tag_movie_rating[2];
                    estimatedMovieRatings.put(movie, Double.valueOf(estimatedRating));
                } else {
                    throw new UnsupportedOperationException("[Error] Unsupported tag" + tag);
                }
            }
            double averageMovieRating = watchedMovieRatingSum / watchedMovieRatings.size();

            for (Map.Entry<String, Double> estimatedMovieRating : estimatedMovieRatings.entrySet()) {
                Double estimatedRating = estimatedMovieRating.getValue();
                String movie = estimatedMovieRating.getKey();
                if (estimatedRating.compareTo(averageMovieRating) >= 0) {
                    context.write(key, new Text(movie + ":" + estimatedRating));
                }
            }
        }
    }

    public static void main (String[] args) throws Exception {
        String userMovieRatingInputPath = args[0];
        String userMovieRatingEstimationInputPath = args[1];
        String recommendedMovieListOutputPath = args[2];

        Configuration conf = new Configuration();
        MapReduceHelper.deleteDirectory(FileSystem.get(conf), new Path(recommendedMovieListOutputPath));
        Job job = new Job(conf);

        job.setJarByClass(RecommendedMovieListGenerator.class);

        MultipleInputs.addInputPath(job, new Path(userMovieRatingInputPath), TextInputFormat.class, WatchedMovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(userMovieRatingEstimationInputPath), TextInputFormat.class, MovieRatingEstimationMapper.class);
        TextOutputFormat.setOutputPath(job, new Path(recommendedMovieListOutputPath));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(RecommendedMovieListGeneratorReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}
