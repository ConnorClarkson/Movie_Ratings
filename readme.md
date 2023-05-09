# Movie Ratings Calculation
#### by Connor Clarkson
## Purpose
A Pyspark application which reads in two data files (movies.data and ratings.dat) 
and calculates two new tables by doing the following.<br>
1. Read in movies.dat and ratings.dat to spark dataframes.
2. Creates a new dataframe, which contains the movies data and 3 new columns max, min and
average rating for that movie from the ratings data.
3. Creates a new dataframe which contains each user’s top 3 movies
based on their rating.
4. Write out the original and new dataframes to parquet.

## Usage

### Assumptions
Developed using python 3.11.<br>
users.dat has been removed due to the data not being used with the program.
Program assumes that the data is in a `./data` folder next to the python module
Program assumes that the development machine has access to pypi or a locally hosted mirror
### Setting up

To setup the python env for running the PySpark app do the following:
```commandline
make setup
```
This will create a virtual environment and install the required packages

### Running the program
To run the program, use the following command assuming you are at the MovieRating folder.
```python
python -m movie_ratings -m /path/to/movies.dat -r /path/to/ratings.dat -o /output/path
```
### Running the tests
To run the unit tests for the module, run the following:
```python
python -m unittest
```
### Spark-Submit
To make the application ready for spark-submit do the following. Change the command line args to suitable locations.
<br>*warning*: spark-submit command is untested. Following spark documentation I came to the below command
```commandline
make sparksubmit
spark-submit --master yarn --deploy-mode client --py-files movie_ratings.zip movie_ratings/__main__.py -m path/to/movies.data -r path/to/ratings.dat -o path/to/output
```

### Improvements
Given the task, I saved the data within the git repo. Normally this isnt best practice and I would either have a action to download and unpack the data from source or assume the data is available on the development machine.
I would add more robust tests to both tables:<br>
Movie_Ratings:<br>
- Edge cases testing for bad data and how the calculation would handle the outcome
Top three:<br>
- A larger test input to handle outside cases such as how would the code handle 4 tied movies etc

I would have liked to test the spark-submit command on a cluster to see the outcome. I currently have no expectations for the command to work or the correct output to be produced.