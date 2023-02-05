# AgingMarkerDatabase
Bioinformatic-fetch utility

The python version used is Python 3.9. There are two python script written for designing the database; “data_parse.py” which contains the codes for parsing the data files given and “AgingMarkerDatabase.py” which is our main program to be called on the command line. The “DataParse_2778496.py” is imported into the main program and called when necessary. The main program (AgingMarkerDatabase.py), the “DataParse_2778496.py” and the data files should be within the same directory before running on a command line. This is because the main program is designed such that all paths to files used and the database file generated as output are in the same directory. Also, the scatterplot image generated as output in query 9 will also be found in the same directory.

The main program is a class “MarkerDatabase” which contain methods for creating, parsing, loading data into database and querying data from the database created. All SQL DDL statements to create database and INSERT statement for loading the data into the database are all written in python. Also, the SQL query statements are all written and the query is done in python with reference to sqlite3 – DB-API documentation (https://docs.python.org/3/library/sqlite3.html). No external SQL script is written. All modules used for written the program are python inbuilt modules, therefore no external module is used. The modules imported for the program include:

Sqlite3; a python module for working with sqlite database
Agparse; a module for parsing command line arguments (the –created, –loaddb, etc.)
Pandas; this module is used for working with dataframe
Matplotlib; for creating scatterplot generated as output in query 9
sys; to print python output on a command line.
re; a python module for writing regex expression.
Warnings; to suppress all future warnings from the program.

An object of the class MarkerDatabase was created and its methods are called including the query database method which take in a SQL query statements and return the results which is then printed on the command line.

There are three methods from the class MarkerDatabase, each represented the command line arguments --createdb, --loaddb and –querydb. The create database method is used to recreate the database schema, load database method is used to insert correct data into the database and query database method is used to return query data from the database and print appropriate output on the command line.
