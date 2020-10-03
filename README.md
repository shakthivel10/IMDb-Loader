
# IMDb-Loader
This project is a Java application that loads the Internet Movie Database (IMDb) into MySQL and MongoDB databases using the public IMDb datasets. 


## Datasets used 

The datasets used are available at https://datasets.imdbws.com

The following files are required: name.basics.tsv.gz, title.basics.tsv.gz, title.ratings.tsv.gz and title.principals.tsv.gz

## Steps to run the application

 1. Create a database in MySQL where the data will be loaded into. 
 2. From the project directory, run: 
 

	 ```
	gradle run --args='<MySQL_Connection_URL> <MySQL_Username> <MySQL_Password> <Path_to_Datasets> <MongoDB_Databasename>'  
	``` 
	 Note: The MySQL_Connection_URL should have the following as URL query parameters: rewriteBatchedStatements=true&useCursorFetch=true

	 Example: 
	 ````
	gradle run --args='jdbc:mysql://localhost:3306/imdb?rewriteBatchedStatements=true&useCursorFetch=true root password /Users/$USER/Downloads/ imdb'
````