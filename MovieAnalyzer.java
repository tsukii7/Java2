import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class MovieAnalyzer {
    Stream<Movie> movieStream;
    private String dataset_path;

    public static class Movie {
        private String Poster_Link;
        private String Series_Title;
        private int Released_Year;
        private String Certificate;  // nullable
        private int Runtime;
        private String[] Genre;
        private double IMDB_Rating;
        private String Overview;
        private int Meta_score; // nullable -1
        private String Director;
        private String Star1;
        private String Star2;
        private String Star3;
        private String Star4;
        private String[] Stars;
        private int No_of_Votes;
        private int Gross; // nullable -1

        public String getPoster_Link() {
            return Poster_Link;
        }

        public String getSeries_Title() {
            return Series_Title;
        }

        public int getReleased_Year() {
            return Released_Year;
        }

        public String getCertificate() {
            return Certificate;
        }

        public int getRuntime() {
            return Runtime;
        }

        public String[] getGenre() {
            return Genre;
        }

        public double getIMDB_Rating() {
            return IMDB_Rating;
        }

        public String getOverview() {
            return Overview;
        }

        public int getMeta_score() {
            return Meta_score;
        }

        public String getDirector() {
            return Director;
        }

        public String getStar1() {
            return Star1;
        }

        public String getStar2() {
            return Star2;
        }

        public String getStar3() {
            return Star3;
        }

        @Override
        public String toString() {
            return "Movie{" +
                    "Poster_Link='" + Poster_Link + '\'' +
                    ", Series_Title='" + Series_Title + '\'' +
                    ", Released_Year=" + Released_Year +
                    ", Certificate='" + Certificate + '\'' +
                    ", Runtime=" + Runtime +
                    ", Genre='" + Genre + '\'' +
                    ", IMDB_Rating=" + IMDB_Rating +
                    ", Overview='" + Overview + '\'' +
                    ", Meta_score=" + Meta_score +
                    ", Director='" + Director + '\'' +
                    ", Star1='" + Star1 + '\'' +
                    ", Star2='" + Star2 + '\'' +
                    ", Star3='" + Star3 + '\'' +
                    ", Star4='" + Star4 + '\'' +
                    ", No_of_Votes=" + No_of_Votes +
                    ", Gross=" + Gross +
                    '}';
        }

        public String getStar4() {
            return Star4;
        }

        public String[] getStars() {
            return Stars;
        }

        public int getNo_of_Votes() {
            return No_of_Votes;
        }

        public int getGross() {
            return Gross;
        }

        public Movie(String Poster_Link, String Series_Title, String Released_Year, String Certificate, String Runtime,
                     String Genre, String IMDB_Rating, String Overview, String Meta_score, String Director, String Star1,
                     String Star2, String Star3, String Star4, String No_of_Votes, String Gross) {
            Poster_Link = removeTerminalQuotes(Poster_Link);
            this.Poster_Link = Poster_Link;
            this.Series_Title = removeTerminalQuotes(Series_Title);
            this.Released_Year = Integer.parseInt(Released_Year);
            this.Certificate = Certificate;
            this.Runtime = Integer.parseInt(Runtime.substring(0, Runtime.length() - 4));
            Genre = removeTerminalQuotes(Genre);
            this.Genre = Genre.split(", ");
            this.IMDB_Rating = Float.parseFloat(IMDB_Rating);
            this.Overview = removeTerminalQuotes(Overview);
            if (Meta_score.length() > 0)
                this.Meta_score = Integer.parseInt(Meta_score);
            else this.Meta_score = -1;
            this.Director = Director;
            this.Star1 = Star1;
            this.Star2 = Star2;
            this.Star3 = Star3;
            this.Star4 = Star4;
            this.Stars = new String[]{Star1, Star2, Star3, Star4};
            Arrays.sort(this.Stars);
            this.No_of_Votes = Integer.parseInt(No_of_Votes);
            Gross = removeTerminalQuotes(Gross);
            Gross = Gross.replace(",", "");
            if (Gross.length() > 0)
                this.Gross = Integer.parseInt(Gross);
            else this.Gross = -1;
        }

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

    public MovieAnalyzer(String dataset_path) throws IOException {
        this.dataset_path = dataset_path;
        readCSV();
//        movies = new ArrayList<Movie>;
        this.movieStream = Files.lines(Paths.get(dataset_path)).skip(1)
                .map(l -> l.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1))
                .map(a -> new Movie(a[0], a[1], a[2], a[3], a[4],
                        a[5], a[6], a[7], a[8], a[9], a[10],
                        a[11], a[12], a[13], a[14], a[15]));
    }
    public void readCSV() throws IOException {
        this.movieStream = Files.lines(Paths.get(dataset_path)).skip(1)
                .map(l -> l.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1))
                .map(a -> new Movie(a[0], a[1], a[2], a[3], a[4],
                        a[5], a[6], a[7], a[8], a[9], a[10],
                        a[11], a[12], a[13], a[14], a[15]));
    }

    public Map<Integer, Integer> getMovieCountByYear() {
        try {
            readCSV();
            Map<Integer, Integer> year_cnt = new LinkedHashMap<>();
            this.movieStream.forEach(m -> year_cnt.put(m.getReleased_Year(), 0));
            readCSV();
            this.movieStream.forEach(m -> year_cnt.put(m.getReleased_Year(), year_cnt.get(m.getReleased_Year()) + 1));
            Stream<Map.Entry<Integer, Integer>> mapStream = year_cnt.entrySet().stream().sorted((o1, o2) -> o2.getKey() - o1.getKey());
            Map<Integer, Integer> reversed_year_cnt = new LinkedHashMap<>();
            mapStream.forEachOrdered(x -> reversed_year_cnt.put(x.getKey(), x.getValue()));
            return reversed_year_cnt;
        } catch (IOException e) {
            System.out.println(e);
        }

        return null;
    }

    public Map<String, Integer> getMovieCountByGenre() {
        try {
            readCSV();
            Map<String, Integer> genre_cnt = new LinkedHashMap<>();
            this.movieStream.forEach(m -> {
                String[] genres = m.getGenre();
                for (int i = 0; i < genres.length; i++) {
                    genre_cnt.put(genres[i], 0);
                }
            });
            readCSV();
            this.movieStream.forEach(m -> {
                String[] genres = m.getGenre();
                for (int i = 0; i < genres.length; i++) {
                    genre_cnt.put(genres[i], genre_cnt.get(genres[i]) + 1);
                }
            });
            Stream<Map.Entry<String, Integer>> mapStream = genre_cnt.entrySet().stream().sorted((o1, o2) -> {
                if (o1.getValue() > o2.getValue())
                    return -1;
                else if (o1.getValue() < o2.getValue())
                    return 1;
                else return o1.getKey().compareTo(o2.getKey());
            });
            Map<String, Integer> reversed_genre_cnt = new LinkedHashMap<>();
            mapStream.forEachOrdered(x -> reversed_genre_cnt.put(x.getKey(), x.getValue()));
            return reversed_genre_cnt;
        } catch (IOException e) {
            System.out.println(e);
        }
        return null;
    }

    public Map<List<String>, Integer> getCoStarCount() {
        try {
            readCSV();
            Map<List<String>, Integer> genre_cnt = new LinkedHashMap<>();
            this.movieStream.forEach(m -> {
                String[] stars = m.getStars();
                genre_cnt.put(List.of(stars[0], stars[1]), 0);
                genre_cnt.put(List.of(stars[0], stars[2]), 0);
                genre_cnt.put(List.of(stars[0], stars[3]), 0);
                genre_cnt.put(List.of(stars[1], stars[2]), 0);
                genre_cnt.put(List.of(stars[1], stars[3]), 0);
                genre_cnt.put(List.of(stars[2], stars[3]), 0);

            });
            readCSV();
            this.movieStream.forEach(m -> {
                String[] stars = m.getStars();
                genre_cnt.put(List.of(stars[0], stars[1]), genre_cnt.get(List.of(stars[0], stars[1])) + 1);
                genre_cnt.put(List.of(stars[0], stars[2]), genre_cnt.get(List.of(stars[0], stars[2])) + 1);
                genre_cnt.put(List.of(stars[1], stars[2]), genre_cnt.get(List.of(stars[1], stars[2])) + 1);
                genre_cnt.put(List.of(stars[0], stars[3]), genre_cnt.get(List.of(stars[0], stars[3])) + 1);
                genre_cnt.put(List.of(stars[1], stars[3]), genre_cnt.get(List.of(stars[1], stars[3])) + 1);
                genre_cnt.put(List.of(stars[2], stars[3]), genre_cnt.get(List.of(stars[2], stars[3])) + 1);

            });
            return genre_cnt;
        } catch (IOException e) {
            System.out.println(e);
        }

        return null;
    }

    public List<String> getTopMovies(int top_k, String by) {
        try {
            readCSV();
            if ("runtime".equals(by)) {
                Map<String[], Integer> title_time = new LinkedHashMap<>();
                this.movieStream.forEach(m ->
                        title_time.put(new String[]{m.getSeries_Title(), m.getReleased_Year() + ""}, m.getRuntime()));
                return getRankedTitleList(top_k, title_time);
            } else if ("overview".equals(by)) {
                Map<String[], Integer> title_overview = new LinkedHashMap<>();
                this.movieStream.forEach(m ->
                        title_overview.put(new String[]{m.getSeries_Title(), m.getReleased_Year() + ""}, m.getOverview().length()));
                return getRankedTitleList(top_k, title_overview);
            }
        } catch (IOException e) {
            System.out.println(e);
        }
        return null;
    }

    private List<String> getRankedTitleList(int top_k, Map<String[], Integer> title_time) {
        Stream<Map.Entry<String[], Integer>> mapStream = title_time.entrySet().stream().sorted((o1, o2) -> {
            if (o1.getValue() > o2.getValue())
                return -1;
            else if (o1.getValue() < o2.getValue())
                return 1;
            else return o1.getKey()[0].compareTo(o2.getKey()[0]);

        });
        List<String> title_by_time = new ArrayList<>();
        mapStream.limit(top_k).forEachOrdered(x -> title_by_time.add(x.getKey()[0]));
        return title_by_time;
    }

    public List<String> getTopStars(int top_k, String by) {
        try {
            readCSV();
            if ("rating".equals(by)) {
                Map<String, Double[]> star_rating = new LinkedHashMap<>();
                this.movieStream.forEach(m -> {
                    String[] stars = m.getStars();
                    for (int i = 0; i < stars.length; i++) {
                        star_rating.put(stars[i],new Double[] {
                            0d,0d
                        });
                    }
                });
                readCSV();
                this.movieStream.forEach(m -> {
                    String[] stars = m.getStars();
                    for (int i = 0; i < stars.length; i++) {
                        Double[] rating_cnt = star_rating.get(stars[i]);
                        star_rating.put(stars[i],new Double[] {
                                rating_cnt[0]+m.getIMDB_Rating(), rating_cnt[1]+1
                        });
                    }
                });
                return getRankedStarList(top_k, star_rating);
            } else if ("gross".equals(by)) {
                    Map<String, Double[]> star_gross = new LinkedHashMap<>();
                    this.movieStream.forEach(m -> {
                        if (m.getGross() != -1) {
                            String[] stars = m.getStars();
                            for (int i = 0; i < stars.length; i++) {
                                star_gross.put(stars[i], new Double[]{
                                        0d, 0d
                                });
                            }
                        }
                    });
                readCSV();
                    this.movieStream.forEach(m -> {
                        if (m.getGross() != -1) {

                            String[] stars = m.getStars();
                            for (int i = 0; i < stars.length; i++) {
                                Double[] gross_cnt = star_gross.get(stars[i]);
                                star_gross.put(stars[i], new Double[]{
                                        gross_cnt[0] + m.getGross(), gross_cnt[1] + 1
                                });
                            }
                        }
                    });
                return getRankedStarList(top_k, star_gross);
            }
        } catch (IOException e) {
            System.out.println(e);
        }
        return null;
    }

    private List<String> getRankedStarList(int top_k, Map<String, Double[]> star_rating) {
        Stream<Map.Entry<String, Double[]>> mapStream = star_rating.entrySet().stream().sorted((o1, o2) -> {
            double o1_rate = o1.getValue()[0]/o1.getValue()[1];
            double o2_rate = o2.getValue()[0]/o2.getValue()[1];
            if (o1_rate > o2_rate)
                return -1;
            else if (o1_rate < o2_rate)
                return 1;
            else return o1.getKey().compareTo(o2.getKey());
        });
        List<String> star_by_rating = new ArrayList<>();
        mapStream.limit(top_k).forEachOrdered(x -> star_by_rating.add(x.getKey()));
        return star_by_rating;
    }

    public List<String> searchMovies(String genre, float min_rating, int max_runtime) {
        try {
            readCSV();
            List<String> searchedMovie = new ArrayList<>();
            this.movieStream.forEach(m -> {
                String genres = Arrays.toString(m.getGenre());
                if (genres.contains(genre) && m.getIMDB_Rating() >= min_rating && m.getRuntime() <= max_runtime)
                    searchedMovie.add(m.getSeries_Title());
            });
            Stream<String> mapStream = searchedMovie.stream().sorted(String::compareTo);
            return mapStream.collect(Collectors.toList());
        } catch (IOException e) {
            System.out.println(e);
        }
        return null;
    }
}