
  

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
	 - personId references Person(id)
	 - movieId references Movie(id)
 - ActedIn (**personId**, **movieId**)
	 - personId references Person(id)
	 - movieId references Movie(id)
 - ComposedBy (**personId**, **movieId**)
	 - personId references Person(id)
	 - movieId references Movie(id)
  
 - DirectedBy (**personId**, **movieId**)
	 - personId references Person(id)
	 - movieId references Movie(id)
  
 - EditedBy (**personId**, **movieId**)
	 - personId references Person(id)
	 - movieId references Movie(id)
  
 - ProducedBy (**personId**, **movieId**)
	 - personId references Person(id)
	 - movieId references Movie(id)
  
 - WrittenBy (**personId**, **movieId**)
	 - personId references Person(id)
	 - movieId references Movie(id)
  
  We use the following schema in MongoDB:

 - Movies ( title, releaseYear, runtime, rating, numberOfVotes, genres[])
 - People (name, birthYear, deathYear, actor[], composer[], director[], producer[], editor[], writer[] )

 
## Steps to run the application


1. Create a database in MySQL where the data will be loaded into.

2. From the project directory, run:
  

```
gradle run --args='<MySQL_connection_URL> <MySQL_username> <MySQL_password> <path_to_datasets> <mongodb_database_name>'
```

Note: The MySQL_Connection_URL should have the following as URL query parameters: rewriteBatchedStatements=true&useCursorFetch=true

  

Example:

````
gradle run --args='jdbc:mysql://localhost:3306/imdb?rewriteBatchedStatements=true&useCursorFetch=true root password /Users/$USER/Downloads/ imdb'
````