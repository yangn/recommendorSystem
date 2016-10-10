package recommendation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/*
 * Sparse Matrix multiplication based on two Mapper and one Reducer
 */
public class NormalizedMultiplication {

    private static final String CO_OCCUR_MATRIX_TAG = "C";
    private static final String USER_MOVIE_RATING_MATRIX_TAG = "R";

    private static int CO_OCCUR_MATRIX_SIZE = 20000; //TODO: make it configurable
    private static int MAX_USER = 1000; //TODO: make it configurable

    public static class CoOccurrenceMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] moviePair_cooccurrence = value.toString().trim().split("\t");
            String moviePair = moviePair_cooccurrence[0];
            String coOccurrence = moviePair_cooccurrence[1];
            String[] movies = moviePair.split(":");
            String movie1 = movies[0];
            String movie2 = movies[1];

            for (int userIdCandidate  = 0; userIdCandidate < MAX_USER; userIdCandidate++) {
                context.write(new Text(movie1 + ":" + userIdCandidate),
                              new Text(CO_OCCUR_MATRIX_TAG + ":" + movie2 + ":" + coOccurrence));
            }
        }
    }

    public static class UserRatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] userId_movieId_rating = value.toString().trim().split(",");
            String userId = userId_movieId_rating[0];
            String currentMovieId = userId_movieId_rating[1];
            String rating = userId_movieId_rating[2];

            for (int movieIdCandidate = 0; movieIdCandidate < CO_OCCUR_MATRIX_SIZE; movieIdCandidate++) {
                context.write(new Text(movieIdCandidate + ":" + userId),
                              new Text(USER_MOVIE_RATING_MATRIX_TAG + ":" + currentMovieId + ":" + rating));
            }
        }
    }

    public static class NormalizedMultiplicationReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text movieIdUserId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //key=movieId:userId value=SOURCE_TAG:movieId:rating

            //Data fetching
            Map<Integer, Double> coOccurIndexedByMovieId = new HashMap();
            Map<Integer, Double> userRatingsIndexedByMovieId = new HashMap();
            long totalCooccurence = 0;
            while (values.iterator().hasNext()) {
                Text value = values.iterator().next();
                String[] valueSplits = value.toString().trim().split(":");
                String tag = valueSplits[0];
                String movieId = valueSplits[1];
                if (USER_MOVIE_RATING_MATRIX_TAG.equals(tag)) { //if data is from user_movie_rating, put to user hash
                    String rating = valueSplits[2];
                    userRatingsIndexedByMovieId.put(Integer.valueOf(movieId), Double.valueOf(rating));
                } else if (CO_OCCUR_MATRIX_TAG.equals(tag)) { //if data is co-occurrence matrix data, put to coOccur hash
                    String coOccurrence = valueSplits[2];
                    coOccurIndexedByMovieId.put(Integer.valueOf(movieId), Double.valueOf(coOccurrence));
                    totalCooccurence += Long.valueOf(coOccurrence); //sum up total cooccurrence of a row for normalization
                } else {
                    throw new UnsupportedOperationException("TAG" + tag + "is not supported");
                }
            }

            //Normalize Cooccurance Matrix
            for (Iterator<Map.Entry<Integer, Double>> iter = coOccurIndexedByMovieId.entrySet().iterator();
                    iter.hasNext();) {
                Map.Entry<Integer, Double> coOccur = iter.next();
                Double coOccurScore = coOccur.getValue();
                coOccurScore = coOccurScore / (double) totalCooccurence; //normalize
                coOccurIndexedByMovieId.put(coOccur.getKey(), coOccurScore);
            }

            //Multiplication
            Double estimatedMovieRating = 0d;
            for (Iterator<Map.Entry<Integer, Double>> iter = userRatingsIndexedByMovieId.entrySet().iterator();
                    iter.hasNext();) {
                Map.Entry<Integer, Double> userRatings = iter.next();
                Integer movieId = userRatings.getKey();
                if (coOccurIndexedByMovieId.containsKey(movieId)) {
                    Double coOccur = coOccurIndexedByMovieId.get(movieId);
                    estimatedMovieRating += coOccur * (double) userRatings.getValue();
                }
            }

            if (estimatedMovieRating.compareTo((double) 0) > 0) {  //filter out 0 to save storage
                DecimalFormat decimalFormat = new DecimalFormat("#.00");
                String formattedEstimatedMovieRating = decimalFormat.format(estimatedMovieRating);
                Text userIdMovieId = swapMovieIdUserId(movieIdUserId);
                context.write(userIdMovieId, new Text(formattedEstimatedMovieRating));
            }
        }
    }

    private static Text swapMovieIdUserId(Text movieIdUserId) {
        Integer userId = Integer.valueOf(
                movieIdUserId.toString().trim().split(":")[1]);
        Integer movieId = Integer.valueOf(
                movieIdUserId.toString().trim().split(":")[0]);
        Text userIdMovieId = new Text(userId + ":" + movieId);
        return new Text(userIdMovieId);
    }

    public static void main(String[] args) throws Exception {
        String userRatingInputPath = args[0];
        String coOccurenceMatrixInputPath = args[1];
        String userRatingEstimationOutputPath = args[2];

        Configuration conf = new Configuration();
        MapReduceHelper.deleteDirectory(FileSystem.get(conf), new Path(userRatingEstimationOutputPath));
        Job job = new Job(conf);
        
        job.setJarByClass(NormalizedMultiplication.class);
        MultipleInputs.addInputPath(job, new Path(userRatingInputPath), TextInputFormat.class, UserRatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(coOccurenceMatrixInputPath), TextInputFormat.class, CoOccurrenceMatrixMapper.class);
        TextOutputFormat.setOutputPath(job, new Path(userRatingEstimationOutputPath));

        job.setReducerClass(NormalizedMultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}
