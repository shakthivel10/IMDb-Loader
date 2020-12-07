

  

  

# IMDb-Loader

  

This project is a Java application that loads the Internet Movie Database (IMDb) into MySQL and MongoDB databases.

## Datasets

  

The datasets used are available at https://datasets.imdbws.com

  

The following files are required: name.basics.tsv.gz, title.basics.tsv.gz, title.ratings.tsv.gz and title.principals.tsv.gz

  

## Data Modeling

We use the following database schema in MySQL, primary keys are in bold:

- Person (**id**, name, birthYear, deathYear)

- Movie (**id**, title, releaseYear, runtime, rating, numberOfVotes)

- Genre (**id**, name)

- HasGenre (**genreId**, **movieId**)

	- **personId** references Person(**id**)

	- **movieId** references Movie(**id**)

- ActedIn (**personId**, **movieId**)

	- **personId** references Person(**id**)

	- **movieId** references Movie(**id**)

- ComposedBy (**personId**, **movieId**)

	- **personId** references Person(**id**)

	- **movieId** references Movie(**id**)

- DirectedBy (**personId**, **movieId**)

	- **personId** references Person(**id**)

	- **movieId** references Movie(**id**)

- EditedBy (**personId**, **movieId**)

	- **personId** references Person(**id**)

	- **movieId** references Movie(**id**)

- ProducedBy (**personId**, **movieId**)

	- **personId** references Person(**id**)

	- **movieId** references Movie(**id**)

- WrittenBy (**personId**, **movieId**)

	- **personId** references Person(**id**)

	- **movieId** references Movie(**id**)

We use the following schema in MongoDB:

  

- Movies ( title, releaseYear, runtime, rating, numberOfVotes, genres[])

- People (name, birthYear, deathYear, actor[], composer[], director[], producer[], editor[], writer[] )

  

## To run the application

  

  

1. Create a database in MySQL where the data will be loaded into.

  

2. From the project directory, run:

  

```

gradle run --args='<MySQL_connection_URL> <MySQL_username> <MySQL_password> <path_to_datasets> <mongodb_database_name>'

```

  

Note: The MySQL_Connection_URL should have the following as URL query parameters: rewriteBatchedStatements=true&useCursorFetch=true

  

For Example,

  

````

gradle run --args='jdbc:mysql://localhost:3306/imdb?rewriteBatchedStatements=true&useCursorFetch=true root password /Users/$USER/Downloads/ imdb'

````

  

## Testing

The databases can be tested by verifying the results of the following queries:

#### MySQL


1. Query: (finds the number of movies loaded in the database)
	```
	SELECT COUNT(*) FROM Movie;
	```

	Expected Result:

	```
	1379465
	```
2. Query: (finds the name of movie with id=109830)
	```
	SELECT title FROM Movie where id=109830;
	```

	Expected Result:

	```
	Forrest Gump
	```

3. Query: (finds the name, birth year and number of movies acted in, before 2019, by person with id=158)

	```
	SELECT name, birthYear, count(*) as numberOfMoviesActed 
	FROM Person as p JOIN ActedIn as a JOIN Movie as m on 
		p.id = a.personId and a.movieId = m.id 
	WHERE p.id=158 and m.releaseYear < 2019;
	```

	Expected Result:

	```
	'Tom Hanks',1956,79
	```

#### MongoDB

1. Aggregation: (finds the number of people in the database)

	```
	db.People.aggregate([{ $count: "count" }])
	```

	Expected Result:

	```
	{ "count" : 9706922 }
	```
2. Aggregation: (finds the name of person with id=138)

	```
	db.People.aggregate([
	{$match:{"_id":138}},
	{$project:{"name":1,"_id":0}}
	])
	```

	Expected Result:

	```
	{ "name" : "Leonardo DiCaprio" }
	```

3. Aggregation: (finds the name, birth year and number of movies acted in, before 2019, by person with id=158)

	```
	db.People.aggregate([
	{$match:{'_id':158}},
	{$lookup:{from:'Movies',localField:'actor',foreignField:'_id',as:'movieInfo'}},
	{$project:{name:1,birthYear:1,movieInfo:{$filter:{input:'$movieInfo',cond:{$lt:['$$this.releaseYear',2019]}}}}},
	{$addFields:{numberOfMovies:{$size:'$movieInfo'}}},
	{$project:{movieInfo:0,_id:0}}
	])
	```

	Expected Result:

	```
	{"name" : "Tom Hanks", "birthYear" : 1956, "numberOfMovies" : 79 }
	```
