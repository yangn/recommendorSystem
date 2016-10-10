package recommendation;

public class Driver {

    public static final String DEFAULT_USER_MOVIE_RATING_PATH = "files/user_movie_ratings";
    public static final String DEFAULT_RECCOMED_MOVIE_LIST_PATH = "files/recommended_movie_list";

    public static final String USER_MOVIE_LIST_PATH = "files/temp/user_movie_list";
    public static final String CO_OCCURRENCE_MATRIX_PATH = "files/temp/co_occurrence_matrix";
    public static final String ESTIMATED_MOVIE_RATING_FOR_USER_PATH = "files/temp/estimated_movie_rating_for_user";

    public static void main(String[] args) throws Exception {

        String userMovieRatingPath = DEFAULT_USER_MOVIE_RATING_PATH;
        String recommendedMovieListPath = DEFAULT_RECCOMED_MOVIE_LIST_PATH;

        if (args.length >= 2 &&
                args[0] != null &&
                args[1] != null) {
            userMovieRatingPath = args[0];
            recommendedMovieListPath = args[1];
        }

        DataDividerByUser dataDividerByUser = new DataDividerByUser();
        CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
        NormalizedMultiplication normalizedMultiplication = new NormalizedMultiplication();
        RecommendedMovieListGenerator recommendedMovieListGenerator = new RecommendedMovieListGenerator();
        dataDividerByUser.main(
                new String[] {userMovieRatingPath, USER_MOVIE_LIST_PATH});
        coOccurrenceMatrixGenerator.main(
                new String[] {USER_MOVIE_LIST_PATH, CO_OCCURRENCE_MATRIX_PATH});
        normalizedMultiplication.main(
                new String[] {userMovieRatingPath, CO_OCCURRENCE_MATRIX_PATH, ESTIMATED_MOVIE_RATING_FOR_USER_PATH});
        recommendedMovieListGenerator.main(
                new String[] {userMovieRatingPath, ESTIMATED_MOVIE_RATING_FOR_USER_PATH, recommendedMovieListPath});
    }
}
