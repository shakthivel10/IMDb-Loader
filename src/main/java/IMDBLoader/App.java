package IMDBLoader;

import java.io.*;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.push;

public class App {

    private static String ConnString;
    private static String USERNAME;
    private static String PASSWORD;
    private static String PathToDataSets;
    private static String MongoDatabase;

    private static InputStream gzipStream;
    private static BufferedReader br;
    private static String line;
    private static HashMap<String, Integer> map;
    private static Connection conn;

    public static void main(String[] args) throws InterruptedException {

        ConnString = args[0];
        USERNAME = args[1];
        PASSWORD = args[2];
        PathToDataSets = args[3];
        MongoDatabase = args[4];

        int counter = 0;

        long start = System.currentTimeMillis();
        long startCommon = start;

        System.out.println("Starting MySQL database insertions ");


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


            System.out.println("Starting MongoDB database Insertions");

            MongoClient client = new MongoClient();
            MongoDatabase mongoDatabase = client.getDatabase(MongoDatabase);

            ResultSet rs;


            // Create collection People and load data from MySQL database

            MongoCollection<Document> PeopleCollection = mongoDatabase.getCollection("People");

            st = conn.prepareStatement("select id, name, birthYear, deathYear from Person");
            st.setFetchSize(4096);

            rs = st.executeQuery();

            List<Document> documentList =  new ArrayList<>();
            List<Integer> emptyList = new ArrayList<>();

            Document document;

            counter = 0;

            while (rs.next()) {

                document = new Document();
                document.append("_id", rs.getInt("id"));
                document.append("name", rs.getString("name"));

                // true birth and death years are not 0

                if ( rs.getInt("birthYear") != 0) {
                    document.append("birthYear", rs.getInt("birthYear"));
                }

                if ( rs.getInt("deathYear") != 0) {
                    document.append("deathYear", rs.getInt("deathYear"));
                }

                document.append("actor",emptyList);
                document.append("composer",emptyList);
                document.append("director",emptyList);
                document.append("editor",emptyList);
                document.append("producer",emptyList);
                document.append("writer",emptyList);

                documentList.add(document);

                if(++counter%4096 == 0) {
                    PeopleCollection.insertMany(documentList);
                    documentList = new ArrayList<>();
                }
            }

            if(documentList.size()!=0) {
                PeopleCollection.insertMany(documentList);
            }

            st.close();
            rs.close();
            documentList = null;

            System.out.println("People Collection Loaded in : "+(float)(System.currentTimeMillis() - start) / 1000 +" second(s)");

            // Create collection Movies and load data from MySQL database

            start = System.currentTimeMillis();

            st = conn.prepareStatement("select id, title, releaseYear, runtime, rating, numberOfVotes from Movie");
            st.setFetchSize(4096);

            rs = st.executeQuery();

            MongoCollection<Document> MoviesCollection = mongoDatabase.getCollection("Movies");

            emptyList = new ArrayList<>();
            documentList =  new ArrayList<>();
            counter = 0;

            while (rs.next()) {

                document = new Document();
                document.append("_id", rs.getInt("id"));
                document.append("title", rs.getString("title"));

                if ( rs.getInt("releaseYear") != 0) {
                    document.append("releaseYear", rs.getInt("releaseYear"));
                }

                if ( rs.getString("runtime") != null) {
                    document.append("runtime", rs.getInt("runtime"));
                }

                if ( rs.getString("rating") != null) {
                    document.append("rating", rs.getFloat("rating"));
                }

                if ( rs.getString("numberOfVotes") != null) {
                    document.append("numberOfVotes", rs.getInt("numberOfVotes"));
                }

                document.append("genres", emptyList);
                documentList.add(document);

                if(++counter%4096 == 0) {
                    MoviesCollection.insertMany(documentList);
                    documentList = new ArrayList<>();
                }
            }

            if(documentList.size()!=0) {
                MoviesCollection.insertMany(documentList);
            }

            st.close();
            rs.close();
            documentList = null;

            pushSQLAssociationsToMongoMoviesCollection(PeopleCollection, "ActedIn", "actor");
            pushSQLAssociationsToMongoMoviesCollection(PeopleCollection, "ComposedBy", "composer");
            pushSQLAssociationsToMongoMoviesCollection(PeopleCollection, "DirectedBy", "director");
            pushSQLAssociationsToMongoMoviesCollection(PeopleCollection, "EditedBy", "editor");
            pushSQLAssociationsToMongoMoviesCollection(PeopleCollection, "ProducedBy", "producer");
            pushSQLAssociationsToMongoMoviesCollection(PeopleCollection, "WrittenBy", "writer");

            st = conn.prepareStatement("select t1.movieId, t2.name from HasGenre as t1 join genre as t2 on t1.genreId = t2.id");
            st.setFetchSize(8192);

            rs = st.executeQuery();

            while (rs.next()) {
                MoviesCollection.updateOne(eq("_id",rs.getInt("t1.movieId")),push("genres", rs.getString("t2.name")));
            }

            st.close();
            rs.close();

            System.out.println("Movies Collection Loaded in : "+(float)(System.currentTimeMillis() - start) / 1000 +" second(s)");

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

    private static void pushSQLAssociationsToMongoMoviesCollection(MongoCollection PeopleCollection, String SQLAssociationTableName, String mongoArrayName) throws SQLException {
        PreparedStatement st = conn.prepareStatement("select * from "+ SQLAssociationTableName);
        st.setFetchSize(8192);
        ResultSet rs = st.executeQuery();

        while (rs.next()) {
            PeopleCollection.updateOne(eq("_id",rs.getInt("personId")),push(mongoArrayName,rs.getInt("movieId")));
        }

        st.close();
        rs.close();
    }
}