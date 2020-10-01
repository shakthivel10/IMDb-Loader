
# IMDb-Loader
This project is a Java application that loads the Internet Movie Database (IMDb) into MySQL and MongoDB databases using the public IMDb datasets. 


## Datasets used 

The datasets used are available at https://datasets.imdbws.com

The following files are required: name.basics.tsv.gz, title.basics.tsv.gz, title.ratings.tsv.gz and title.principals.tsv.gz

## Steps to run the application

 1. Create a database in MySQL where the data will be loaded into. 
 2. From the project directory, run: 
	 ```
	gradle run --args='<Connection_URL> <DB_Username> <DB_Password> <Path_to_Datasets>'
	``` 
	Additionally, the Connection_URL show have 'rewriteBatchedStatements=true' as a URL query parameter.

	 Example: 
	 ````
	gradle run --args='jdbc:mysql://localhost:3306/imdb?rewriteBatchedStatements=true root password /Users/$USER/Downloads';
````