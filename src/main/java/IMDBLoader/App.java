package IMDBLoader;

import java.io.*;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.Connection;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

public class App {

    private static String ConnString;
    private static String USERNAME;
    private static String PASSWORD;
    private static String PathToDataSets;

    private static InputStream gzipStream;
    private static BufferedReader br;
    private static String line;
    private static HashMap<String, Integer> map;

    public static void main(String[] args) throws InterruptedException {

        ConnString = args[0];
        USERNAME = args[1];
        PASSWORD = args[2];
        PathToDataSets = args[3];
        String MongoConnString = args[4] ;
        String MongoDatabase = args[5];

        int counter = 0;

        long start = System.currentTimeMillis();
        long startCommon = start;

        Connection conn = null;
        PreparedStatement st, st2, st3, st4, st5, st6;

        try {

            conn = DriverManager.getConnection(ConnString, USERNAME, PASSWORD);
            conn.setAutoCommit(false);

            // Create and load data from name.basics.tsv.gz into Person Table

            st = conn.prepareStatement(
                    "create table Person(id int(10), name varchar(300), birthYear int(4), deathYear int(4), PRIMARY KEY(id))");

            st.addBatch();
            st.executeBatch();
            conn.commit();

            gzipStream = new GZIPInputStream(new FileInputStream(PathToDataSets + "name.basics.tsv.gz"));

            br = new BufferedReader(new InputStreamReader(gzipStream));
            br.readLine();

            st.close();

            st = conn.prepareStatement("insert into Person(id,name,birthYear,deathYear)" + " values(?,?,?,?)");

            while ((line = br.readLine()) != null) {

                String[] arr = line.split("\\t");

                st.setInt(1, Integer.parseInt(arr[0].substring(2)));

                if (!arr[1].equals("\\N"))
                    st.setString(2, arr[1]);
                else
                    st.setNull(2, Types.VARCHAR);

                if (!arr[2].equals("\\N"))
                    st.setInt(3, Integer.parseInt(arr[2]));
                else
                    st.setNull(3, Types.INTEGER);

                if (!arr[3].equals("\\N"))
                    st.setInt(4, Integer.parseInt(arr[3]));
                else
                    st.setNull(4, Types.INTEGER);

                st.addBatch();

                if (counter++ % 4096 == 0) {
                    st.executeBatch();
                    conn.commit();
                }
            }

            st.executeBatch();
            conn.commit();
            st.close();
            br.close();

            System.out.println("Person Table Loaded in " + (float) (System.currentTimeMillis() - start) / 1000 +" second(s)") ;

            // Create Movie, Genre and HasGenre Tables

            start = System.currentTimeMillis();

            st = conn.prepareStatement(
                    "create table Movie( id int(10), title varchar(400), releaseYear int(4), runtime int(10), rating float(4), numberOfVotes int(10), PRIMARY KEY(id))");
            st.addBatch();
            st.executeBatch();
            conn.commit();

            st2 = conn.prepareStatement("create table Genre(id int(10), name varchar(100), PRIMARY KEY(id))");

            st2.addBatch();
            st2.executeBatch();
            conn.commit();

            st3 = conn.prepareStatement(
                    "create table HasGenre(genreId int(10), movieId int(100), PRIMARY KEY(genreId,movieId), FOREIGN KEY(genreId) REFERENCES Genre(id), FOREIGN KEY(movieId) REFERENCES Movie(id))");

            st3.addBatch();
            st3.executeBatch();
            conn.commit();

            st.close();
            st2.close();
            st3.close();

            // Load data into title.basics.tsv.gz Movie, Genre and HasGenre Tables

            gzipStream = new GZIPInputStream(new FileInputStream(PathToDataSets + "title.basics.tsv.gz"));

            br = new BufferedReader(new InputStreamReader(gzipStream));
            br.readLine();

            st = conn.prepareStatement("insert into Movie(id,title,releaseYear,runtime)  values(?,?,?,?)");

            st2 = conn.prepareStatement("insert into Genre(id,name)  values(?,?)");

            st3 = conn.prepareStatement("insert into HasGenre(genreId,movieId)  values(?,?)");

            counter = 0;

            map = new HashMap<>();
            int genreCounter = 1;

            while ((line = br.readLine()) != null) {
                String[] arr = line.split("\\t");

                int movieId;
                if (arr[1].equals("movie") || arr[1].equals("short") || arr[1].equals("tvShort")
                        || arr[1].equals("tvMovie")) {

                    movieId = Integer.parseInt(arr[0].substring(2));

                    st.setInt(1, movieId);

                    if (!arr[3].equals("\\N"))
                        st.setString(2, arr[3]);
                    else
                        st.setNull(2, Types.VARCHAR);

                    if (!arr[5].equals("\\N"))
                        st.setInt(3, Integer.parseInt(arr[5]));
                    else
                        st.setNull(3, Types.INTEGER);

                    if (!arr[7].equals("\\N"))
                        st.setInt(4, Integer.parseInt(arr[7]));
                    else
                        st.setNull(4, Types.INTEGER);

                    st.addBatch();

                    if (counter++ % 1024 == 0) {
                        st.executeBatch();
                        st2.executeBatch();
                        st3.executeBatch();
                        conn.commit();
                    }

                    String[] genres = arr[8].split(",");

                    for (String genre : genres) {
                        int genreId;
                        if (map.containsKey(genre)) {
                            genreId = map.get(genre);

                        } else {
                            map.put(genre, genreCounter);
                            genreId = genreCounter++;
                            st2.setInt(1, genreId);
                            st2.setString(2, genre);
                            st2.addBatch();
                        }

                        st3.setInt(1, genreId);
                        st3.setInt(2, movieId);
                        st3.addBatch();
                    }
                }
            }

            st.executeBatch();
            st2.executeBatch();
            st3.executeBatch();
            conn.commit();

            st.close();
            st2.close();
            st3.close();
            br.close();

            System.out.println("Movies, Genre and HasGenre Tables Loaded in "
                    + (float) (System.currentTimeMillis() - start) / 1000 +" second(s)");

            // Update Ratings, Number Of Votes from title.ratings.tsv.gz in Movies Table

            start = System.currentTimeMillis();

            gzipStream = new GZIPInputStream(new FileInputStream(PathToDataSets + "title.ratings.tsv.gz"));

            br = new BufferedReader(new InputStreamReader(gzipStream));
            br.readLine();

            st = conn.prepareStatement("UPDATE Movie SET rating=?,numberOfVotes=? where id=?");

            while ((line = br.readLine()) != null) {

                String[] arr = line.split("\\t");

                st.setFloat(1, Float.parseFloat(arr[1]));

                st.setInt(2, Integer.parseInt(arr[2]));

                st.setInt(3, Integer.parseInt(arr[0].substring(2)));

                st.addBatch();

                if (counter++ % 4096 == 0) {
                    st.executeBatch();
                    conn.commit();
                }

            }

            st.executeBatch();
            conn.commit();

            st.close();
            br.close();

            System.out.println(
                    "Ratings, Number Of Votes updated in " + (float) (System.currentTimeMillis() - start) / 1000 + " second(s)");

            start = System.currentTimeMillis();

            st = conn.prepareStatement("create table ActedIn( personId int(10)," + "movieId int(10),"
                    + "PRIMARY KEY (personId,movieId)," + "FOREIGN KEY(personId) REFERENCES Person(Id),"
                    + "FOREIGN KEY(movieId) REFERENCES Movie(Id))");

            st.addBatch();
            st.executeBatch();
            conn.commit();

            st2 = conn.prepareStatement("create table ComposedBy( personId int(10)," + "movieId int(10),"
                    + "PRIMARY KEY (personId,movieId)," + "FOREIGN KEY(personId) REFERENCES Person(Id),"
                    + "FOREIGN KEY(movieId) REFERENCES Movie(Id))");

            st2.addBatch();
            st2.executeBatch();
            conn.commit();

            st3 = conn.prepareStatement("create table DirectedBy( personId int(10)," + "movieId int(10),"
                    + "PRIMARY KEY (personId,movieId)," + "FOREIGN KEY(personId) REFERENCES Person(Id),"
                    + "FOREIGN KEY(movieId) REFERENCES Movie(Id))");

            st3.addBatch();
            st3.executeBatch();
            conn.commit();

            st4 = conn.prepareStatement("create table EditedBy( personId int(10)," + "movieId int(10),"
                    + "PRIMARY KEY (personId,movieId)," + "FOREIGN KEY(personId) REFERENCES Person(Id),"
                    + "FOREIGN KEY(movieId) REFERENCES Movie(Id))");

            st4.addBatch();
            st4.executeBatch();
            conn.commit();

            st5 = conn.prepareStatement("create table ProducedBy( personId int(10)," + "movieId int(10),"
                    + "PRIMARY KEY (personId,movieId)," + "FOREIGN KEY(personId) REFERENCES Person(Id),"
                    + "FOREIGN KEY(movieId) REFERENCES Movie(Id))");

            st5.addBatch();
            st5.executeBatch();
            conn.commit();

            st6 = conn.prepareStatement("create table WrittenBy(personId int(10)," + "movieId int(10),"
                    + "PRIMARY KEY (personId,movieId)," + "FOREIGN KEY(personId) REFERENCES Person(Id),"
                    + "FOREIGN KEY(movieId) REFERENCES Movie(Id))");

            st6.addBatch();
            st6.executeBatch();
            conn.commit();

            st.close();
            st2.close();
            st3.close();
            st4.close();
            st5.close();
            st6.close();

            // Insert associations from title.principals.tsv.gz into ActedIn, DirectedBy, WrittenBy, ComposedBy, EditedBy and ProducedBy Tables

            gzipStream = new GZIPInputStream(new FileInputStream(PathToDataSets + "title.principals.tsv.gz"));

            br = new BufferedReader(new InputStreamReader(gzipStream));
            br.readLine();

            st = conn.prepareStatement("insert Ignore into ActedIn(personId,movieId)"
                    + " select Person.id,Movie.id from Person, Movie where Person.id=? and Movie.id=?");

            st2 = conn.prepareStatement("insert ignore into ComposedBy(personId,movieId)"
                    + " select Person.id,Movie.id from Person, Movie where Person.id=? and Movie.id=?");

            st3 = conn.prepareStatement("insert ignore into DirectedBy(personId,movieId)"
                    + " select Person.id,Movie.id from Person, Movie where Person.id=? and Movie.id=?");

            st4 = conn.prepareStatement("insert ignore into EditedBy(personId,movieId)"
                    + " select Person.id,Movie.id from Person, Movie where Person.id=? and Movie.id=?");

            st5 = conn.prepareStatement("insert ignore into ProducedBy(personId,movieId)"
                    + " select Person.id,Movie.id from Person, Movie where Person.id=? and Movie.id=?");

            st6 = conn.prepareStatement("insert ignore into WrittenBy(personId,movieId)"
                    + " select Person.id,Movie.id from Person, Movie where Person.id=? and Movie.id=?");

            counter = 0;

            long start2 = System.currentTimeMillis();
            int movieId, personId;
            while ((line = br.readLine()) != null) {
                String arr[] = line.split("\\t");

                movieId = Integer.parseInt(arr[0].substring(2));
                personId = Integer.parseInt(arr[2].substring(2));

                switch (arr[3]) {

                    case "actor":
                    case "actress":
                    case "self":
                        st.setInt(1, personId);
                        st.setInt(2, movieId);
                        st.addBatch();
                        break;

                    case "composer":
                        st2.setInt(1, personId);
                        st2.setInt(2, movieId);
                        st2.addBatch();
                        break;

                    case "director":
                        st3.setInt(1, personId);
                        st3.setInt(2, movieId);
                        st3.addBatch();
                        break;

                    case "editor":
                        st4.setInt(1, personId);
                        st4.setInt(2, movieId);
                        st4.addBatch();
                        break;

                    case "producer":
                        st5.setInt(1, personId);
                        st5.setInt(2, movieId);
                        st5.addBatch();
                        break;

                    case "writer":
                        st6.setInt(1, personId);
                        st6.setInt(2, movieId);
                        st6.addBatch();
                        break;
                }

                if (counter++ % 3000 == 0) {
                    st.executeBatch();
                    st2.executeBatch();
                    st3.executeBatch();
                    st4.executeBatch();
                    st5.executeBatch();
                    st6.executeBatch();
                    conn.commit();

                }
            }

            st.executeBatch();
            st2.executeBatch();
            st3.executeBatch();
            st4.executeBatch();
            st5.executeBatch();
            st6.executeBatch();
            conn.commit();

            st.close();
            st2.close();
            st3.close();
            st4.close();
            st5.close();
            st6.close();

            br.close();

            System.out
                    .println("ActedIn, DirectedBy, WrittenBy, ComposedBy, EditedBy and ProducedBy Tables loaded in "
                            + (float) (System.currentTimeMillis() - start) / 1000 +" second(s)");

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } finally {

            System.out.println("Total Time taken by Application to run: "
                    + (float) (System.currentTimeMillis() - startCommon) / 1000 +" second(s)");

            try {

                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
