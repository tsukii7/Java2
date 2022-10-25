import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class MovieAnalyzer {
    Stream<Movie> movieStream;
    private String datasetPath;

    /**
     * Movie class to store the relevant information.
     */
    public static class Movie {
        private String posterLink;
        private String seriesTitle;
        private int releasedYear;
        private String certificate;  // nullable
        private int runtime;
        private String[] genre;
        private double imdbRating;
        private String overview;
        private int metaScore; // nullable -1
        private String director;
        private String star1;
        private String star2;
        private String star3;
        private String star4;
        private String[] stars;
        private int noOfVotes;
        private int gross; // nullable -1

        public String getPosterLink() {
            return posterLink;
        }

        public String getSeriesTitle() {
            return seriesTitle;
        }

        public int getReleasedYear() {
            return releasedYear;
        }

        public String getCertificate() {
            return certificate;
        }

        public int getRuntime() {
            return runtime;
        }

        public String[] getGenre() {
            return genre;
        }

        public double getImdbRating() {
            return imdbRating;
        }

        public String getOverview() {
            return overview;
        }

        public int getMetaScore() {
            return metaScore;
        }

        public String getDirector() {
            return director;
        }

        public String getStar1() {
            return star1;
        }

        public String getStar2() {
            return star2;
        }

        public String getStar3() {
            return star3;
        }

        @Override
        public String toString() {
            return "Movie{"
                    + "Poster_Link='" + posterLink + '\''
                    + ", Series_Title='" + seriesTitle + '\''
                    + ", Released_Year=" + releasedYear
                    + ", Certificate='" + certificate + '\''
                    + ", Runtime=" + runtime
                    + ", Genre='" + Arrays.toString(genre) + '\''
                    + ", IMDB_Rating=" + imdbRating
                    + ", Overview='" + overview + '\''
                    + ", Meta_score=" + metaScore
                    + ", Director='" + director + '\''
                    + ", Star1='" + star1 + '\''
                    + ", Star2='" + star2 + '\''
                    + ", Star3='" + star3 + '\''
                    + ", Star4='" + star4 + '\''
                    + ", No_of_Votes=" + noOfVotes
                    + ", Gross=" + gross
                    + '}';
        }

        public String getStar4() {
            return star4;
        }

        public String[] getStars() {
            return stars;
        }

        public int getNoOfVotes() {
            return noOfVotes;
        }

        public int getGross() {
            return gross;
        }

        /**
         * Creates the Movie instance based on the given parameters.
         */
        public Movie(String posterLink, String seriesTitle, String releasedYear,
                     String certificate, String runtime, String genre, String imdbRating,
                     String overview, String metaScore, String director, String star1,
                     String star2, String star3, String star4, String noOfVotes, String gross) {
            posterLink = removeTerminalQuotes(posterLink);
            this.posterLink = posterLink;
            this.seriesTitle = removeTerminalQuotes(seriesTitle);
            this.releasedYear = Integer.parseInt(releasedYear);
            this.certificate = certificate;
            this.runtime = Integer.parseInt(runtime.substring(0, runtime.length() - 4));
            genre = removeTerminalQuotes(genre);
            this.genre = genre.split(", ");
            this.imdbRating = Float.parseFloat(imdbRating);
            this.overview = removeTerminalQuotes(overview);
            if (metaScore.length() > 0) {
                this.metaScore = Integer.parseInt(metaScore);
            } else {
                this.metaScore = -1;
            }
            this.director = director;
            this.star1 = star1;
            this.star2 = star2;
            this.star3 = star3;
            this.star4 = star4;
            this.stars = new String[]{star1, star2, star3, star4};
            Arrays.sort(this.stars);
            this.noOfVotes = Integer.parseInt(noOfVotes);
            gross = removeTerminalQuotes(gross);
            gross = gross.replace(",", "");
            if (gross.length() > 0) {
                this.gross = Integer.parseInt(gross);

            } else {
                this.gross = -1;
            }
        }

        /**
         * process the string to excise the terminal quotes.
         *
         * @param string string to be processed
         * @return string with quotes
         */
        public String removeTerminalQuotes(String string) {
            if (string.startsWith("\"")) {
                string = string.substring(1);
            }
            if (string.endsWith("\"")) {
                string = string.substring(0, string.length() - 1);
            }
            return string;
        }


    }

    public MovieAnalyzer(String datasetPath) throws IOException {
        this.datasetPath = datasetPath;
        readPath();
        this.movieStream = Files.lines(Paths.get(datasetPath)).skip(1)
                .map(l -> l.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1))
                .map(a -> new Movie(a[0], a[1], a[2], a[3], a[4],
                        a[5], a[6], a[7], a[8], a[9], a[10],
                        a[11], a[12], a[13], a[14], a[15]));
    }

    public void readPath() throws IOException {
        this.movieStream = Files.lines(Paths.get(datasetPath)).skip(1)
                .map(l -> l.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1))
                .map(a -> new Movie(a[0], a[1], a[2], a[3], a[4],
                        a[5], a[6], a[7], a[8], a[9], a[10],
                        a[11], a[12], a[13], a[14], a[15]));
    }

    public Map<Integer, Integer> getMovieCountByYear() {
        try {
            readPath();
            Map<Integer, Integer> yearCnt = new LinkedHashMap<>();
            this.movieStream.forEach(m -> yearCnt.put(m.getReleasedYear(), 0));
            readPath();
            this.movieStream.forEach(m ->
                    yearCnt.put(m.getReleasedYear(), yearCnt.get(m.getReleasedYear()) + 1));
            Stream<Map.Entry<Integer, Integer>> mapStream = yearCnt.entrySet().stream().sorted(
                    (o1, o2) -> o2.getKey() - o1.getKey());
            Map<Integer, Integer> reversedYearCnt = new LinkedHashMap<>();
            mapStream.forEachOrdered(x -> reversedYearCnt.put(x.getKey(), x.getValue()));
            return reversedYearCnt;
        } catch (IOException e) {
            System.out.println(e);
        }

        return null;
    }

    public Map<String, Integer> getMovieCountByGenre() {
        try {
            readPath();
            Map<String, Integer> genreCnt = new LinkedHashMap<>();
            this.movieStream.forEach(m -> {
                String[] genres = m.getGenre();
                for (String genre : genres) {
                    genreCnt.put(genre, 0);
                }
            });
            readPath();
            this.movieStream.forEach(m -> {
                String[] genres = m.getGenre();
                for (String genre : genres) {
                    genreCnt.put(genre, genreCnt.get(genre) + 1);
                }
            });
            Stream<Map.Entry<String, Integer>> mapStream = genreCnt.entrySet().stream().sorted(
                    (o1, o2) -> {
                        if (o1.getValue() > o2.getValue()) {
                            return -1;
                        } else if (o1.getValue() < o2.getValue()) {
                            return 1;
                        } else {
                            return o1.getKey().compareTo(o2.getKey());
                        }
                    });
            Map<String, Integer> reversedGenreCnt = new LinkedHashMap<>();
            mapStream.forEachOrdered(x -> reversedGenreCnt.put(x.getKey(), x.getValue()));
            return reversedGenreCnt;
        } catch (IOException e) {
            System.out.println(e);
        }
        return null;
    }

    public Map<List<String>, Integer> getCoStarCount() {
        try {
            readPath();
            Map<List<String>, Integer> genreCnt = new LinkedHashMap<>();
            this.movieStream.forEach(m -> {
                String[] stars = m.getStars();
                genreCnt.put(List.of(stars[0], stars[1]), 0);
                genreCnt.put(List.of(stars[0], stars[2]), 0);
                genreCnt.put(List.of(stars[0], stars[3]), 0);
                genreCnt.put(List.of(stars[1], stars[2]), 0);
                genreCnt.put(List.of(stars[1], stars[3]), 0);
                genreCnt.put(List.of(stars[2], stars[3]), 0);

            });
            readPath();
            this.movieStream.forEach(m -> {
                String[] stars = m.getStars();
                genreCnt.put(List.of(stars[0], stars[1]),
                        genreCnt.get(List.of(stars[0], stars[1])) + 1);
                genreCnt.put(List.of(stars[0], stars[2]),
                        genreCnt.get(List.of(stars[0], stars[2])) + 1);
                genreCnt.put(List.of(stars[1], stars[2]),
                        genreCnt.get(List.of(stars[1], stars[2])) + 1);
                genreCnt.put(List.of(stars[0], stars[3]),
                        genreCnt.get(List.of(stars[0], stars[3])) + 1);
                genreCnt.put(List.of(stars[1], stars[3]),
                        genreCnt.get(List.of(stars[1], stars[3])) + 1);
                genreCnt.put(List.of(stars[2], stars[3]),
                        genreCnt.get(List.of(stars[2], stars[3])) + 1);

            });
            return genreCnt;
        } catch (IOException e) {
            System.out.println(e);
        }

        return null;
    }


    public List<String> getTopMovies(int topK, String by) {
        try {
            readPath();
            if ("runtime".equals(by)) {
                Map<String[], Integer> titleTime = new LinkedHashMap<>();
                this.movieStream.forEach(m ->
                        titleTime.put(
                                new String[]{m.getSeriesTitle(), m.getReleasedYear() + ""},
                                m.getRuntime()));
                return getRankedTitleList(topK, titleTime);
            } else if ("overview".equals(by)) {
                Map<String[], Integer> titleOverview = new LinkedHashMap<>();
                this.movieStream.forEach(m ->
                        titleOverview.put(
                                new String[]{m.getSeriesTitle(), m.getReleasedYear() + ""},
                                m.getOverview().length()));
                return getRankedTitleList(topK, titleOverview);
            }
        } catch (IOException e) {
            System.out.println(e);
        }
        return null;
    }


    private List<String> getRankedTitleList(int topK, Map<String[], Integer> titleTime) {
        Stream<Map.Entry<String[], Integer>> mapStream = titleTime.entrySet().stream().sorted(
                (o1, o2) -> {
                    if (o1.getValue() > o2.getValue()) {
                        return -1;
                    } else if (o1.getValue() < o2.getValue()) {
                        return 1;
                    } else {
                        return o1.getKey()[0].compareTo(o2.getKey()[0]);
                    }

                });
        List<String> titleByTime = new ArrayList<>();
        mapStream.limit(topK).forEachOrdered(x -> titleByTime.add(x.getKey()[0]));
        return titleByTime;
    }

    public List<String> getTopStars(int topK, String by) {
        try {
            readPath();
            if ("rating".equals(by)) {
                Map<String, Double[]> starRating = new LinkedHashMap<>();
                this.movieStream.forEach(m -> {
                    String[] stars = m.getStars();
                    for (String star : stars) {
                        starRating.put(star, new Double[]{0d, 0d});
                    }
                });
                readPath();
                this.movieStream.forEach(m -> {
                    String[] stars = m.getStars();
                    for (String star : stars) {
                        Double[] ratingCnt = starRating.get(star);
                        starRating.put(star, new Double[]{
                                ratingCnt[0] + m.getImdbRating(), ratingCnt[1] + 1
                        });
                    }
                });
                return getRankedStarList(topK, starRating);
            } else if ("gross".equals(by)) {
                Map<String, Double[]> starGross = new LinkedHashMap<>();
                this.movieStream.forEach(m -> {
                    if (m.getGross() != -1) {
                        String[] stars = m.getStars();
                        for (String star : stars) {
                            starGross.put(star, new Double[]{0d, 0d});
                        }
                    }
                });
                readPath();
                this.movieStream.forEach(m -> {
                    if (m.getGross() != -1) {

                        String[] stars = m.getStars();
                        for (String star : stars) {
                            Double[] grossCnt = starGross.get(star);
                            starGross.put(star, new Double[]{
                                    grossCnt[0] + m.getGross(), grossCnt[1] + 1
                            });
                        }
                    }
                });
                return getRankedStarList(topK, starGross);
            }
        } catch (IOException e) {
            System.out.println(e);
        }
        return null;
    }

    private List<String> getRankedStarList(int topK, Map<String, Double[]> starRating) {
        Stream<Map.Entry<String, Double[]>> mapStream = starRating.entrySet().stream().sorted(
                (o1, o2) -> {
                    double o1Rate = o1.getValue()[0] / o1.getValue()[1];
                    double o2Rate = o2.getValue()[0] / o2.getValue()[1];
                    if (o1Rate > o2Rate) {
                        return -1;
                    } else if (o1Rate < o2Rate) {
                        return 1;
                    } else {
                        return o1.getKey().compareTo(o2.getKey());
                    }
                });
        List<String> starByRating = new ArrayList<>();
        mapStream.limit(topK).forEachOrdered(x -> starByRating.add(x.getKey()));
        return starByRating;
    }

    public List<String> searchMovies(String genre, float minRating, int maxRuntime) {
        try {
            readPath();
            List<String> searchedMovie = new ArrayList<>();
            this.movieStream.forEach(m -> {
                String genres = Arrays.toString(m.getGenre());
                if (genres.contains(genre)
                        && m.getImdbRating() >= minRating && m.getRuntime() <= maxRuntime) {
                    searchedMovie.add(m.getSeriesTitle());
                }
            });
            Stream<String> mapStream = searchedMovie.stream().sorted(String::compareTo);
            return mapStream.collect(Collectors.toList());
        } catch (IOException e) {
            System.out.println(e);
        }
        return null;
    }
}