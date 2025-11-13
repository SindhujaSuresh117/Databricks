# Databricks notebook source
#SQL
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

rating = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sureshsindhuja001@gmail.com/rating-1.csv")
movie_cast = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sureshsindhuja001@gmail.com/movie_cast-1.csv")
movie_direction = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sureshsindhuja001@gmail.com/movie_direction-1.csv")
reviewer = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sureshsindhuja001@gmail.com/reviewer-1.csv")
movie = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sureshsindhuja001@gmail.com/movie-1.csv")
movie_genre = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sureshsindhuja001@gmail.com/movie_genres-1.csv")
genre = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sureshsindhuja001@gmail.com/genre-1.csv")
director = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sureshsindhuja001@gmail.com/director-1.csv")
actor = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sureshsindhuja001@gmail.com/actor-1.csv")

rating.createOrReplaceGlobalTempView("rating_table")
movie_cast.createOrReplaceGlobalTempView("movie_cast_table")
movie_direction.createOrReplaceGlobalTempView("movie_direction_table")
reviewer.createOrReplaceGlobalTempView("reviewer_table")
movie.createOrReplaceGlobalTempView("movie_table")
movie_genre.createOrReplaceGlobalTempView("movie_genre_table")
genre.createOrReplaceGlobalTempView("genre_table")
director.createOrReplaceGlobalTempView("director_table")
actor.createOrReplaceGlobalTempView("actor_table")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Global_temp.reviewer_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Global_temp.rating_table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Global_temp.movie_cast_table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Global_temp.movie_direction_table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Global_temp.movie_table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Global_temp.movie_genre_table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Global_temp.genre_table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Global_temp.director_table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Global_temp.actor_table

# COMMAND ----------

# MAGIC %sql
# MAGIC --  1.From the following table, write a SQL query to find all reviewers whose ratings contain a NULL value. Return reviewer name.
# MAGIC
# MAGIC SELECT reviewer_table.rev_id,
# MAGIC         reviewer_table.rev_name,
# MAGIC         rating_table.rev_stars
# MAGIC FROM global_temp.reviewer_table
# MAGIC INNER JOIN Global_temp.rating_table 
# MAGIC ON reviewer_table.rev_id = rating_table.rev_id
# MAGIC WHERE rating_table.rev_stars IS NULL;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 2. From the following table, write a SQL query to find out who was cast in the movie 'Annie Hall'. Return actor first name, last name and role
# MAGIC
# MAGIC SELECT ACT.act_fname,ACT.act_lname,MCT.role
# MAGIC FROM Global_temp.actor_table AS ACT
# MAGIC INNER JOIN global_temp.movie_cast_table AS MCT
# MAGIC ON MCT.act_id = ACT.act_id
# MAGIC INNER JOIN Global_temp.movie_table AS MOV
# MAGIC ON MOV.mov_id = MCT.mov_id
# MAGIC WHERE MOV.mov_title == 'Annie Hall';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4. From the following table, write a SQL query to find out which actors have not appeared in any movies between 1990 and 2000 (Begin and end values are included.). Return actor first name, last name, movie title and release year.
# MAGIC
# MAGIC SELECT ACT.act_fname ,ACT.act_lname,MOV.mov_title,MOV.mov_year
# MAGIC FROM Global_temp.actor_table AS ACT
# MAGIC INNER JOIN global_temp.movie_cast_table AS MCT
# MAGIC ON MCT.act_id = ACT.act_id
# MAGIC INNER JOIN Global_temp.movie_table AS MOV
# MAGIC ON MOV.mov_id = MCT.mov_id
# MAGIC WHERE (MOV.mov_year <= 1990 OR MOV.mov_year >= 2000);

# COMMAND ----------

# MAGIC %sql
# MAGIC  -- 5. From the following tables, write a SQL query to find the movies released before 1st January 1989. Sort the result-set in descending order by date of release. Return movie title, release year, date of release, duration, and first and last name of the director.
# MAGIC
# MAGIC SELECT DT.dir_fname,DT.dir_lname,MOV.mov_title,MOV.mov_year,MOV.mov_time
# MAGIC FROM Global_temp.movie_table AS MOV
# MAGIC INNER JOIN global_temp.movie_direction_table AS MDT
# MAGIC ON MOV.mov_id = MDT.mov_id
# MAGIC INNER JOIN Global_temp.director_table AS DT
# MAGIC ON MDT.dir_id = DT.dir_id
# MAGIC WHERE MOV.mov_year <'1989-01-01'
# MAGIC ORDER BY MOV.mov_year DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 6. From the following table, write a SQL query to calculate the average movie length and count the number of movies in each genre. Return genre title, average time and number of movies for each genre.
# MAGIC SELECT genre_table.gen_title, AVG(movie_table.mov_time) AS AVG_MOV_TIME,COUNT(genre_table.gen_title) AS NUMBER_OF_GEN_TITLE
# MAGIC FROM Global_temp.movie_table
# MAGIC INNER JOIN Global_temp.movie_genre_table 
# MAGIC ON movie_table.mov_id = movie_genre_table.mov_id
# MAGIC INNER JOIN Global_temp.genre_table
# MAGIC ON movie_genre_table.gen_id = genre_table.gen_id
# MAGIC GROUP BY genre_table.gen_title;
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,correlation sub query
# MAGIC %sql
# MAGIC -- 7. From the following table, write a SQL query to find movies with the shortest duration. Return movie title, movie year, director first name, last name, actor first name, last name and role.
# MAGIC
# MAGIC SELECT movie_table.mov_title,movie_table.mov_time,movie_table.mov_year,director_table.dir_fname,director_table.dir_lname,
# MAGIC                  actor_table.act_fname,actor_table.act_lname, movie_cast_table.role
# MAGIC FROM Global_temp.movie_table
# MAGIC INNER JOIN Global_temp.movie_direction_table ON movie_table.mov_id = movie_direction_table.mov_id
# MAGIC INNER JOIN Global_temp.director_table ON director_table.dir_id = movie_direction_table.dir_id
# MAGIC INNER JOIN Global_temp.movie_cast_table ON movie_cast_table.mov_id = movie_table.mov_id
# MAGIC INNER JOIN Global_temp.actor_table ON actor_table.act_id = movie_cast_table.act_id
# MAGIC WHERE movie_table.mov_time = (SELECT MIN(cast(movie_table.mov_time AS INTEGER))FROM Global_temp.movie_table);
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,window function with inner and outer query
# MAGIC %sql
# MAGIC -- 7)solution -2
# MAGIC --notes: order by col_name (in order to cast it in place of col name shld be replaced with cast(col_name))
# MAGIC --order by cast(col_name AS data_type)
# MAGIC SELECT * FROM 
# MAGIC (
# MAGIC SELECT movie_table.mov_title,movie_table.mov_time,movie_table.mov_year,director_table.dir_fname,director_table.dir_lname,
# MAGIC                  actor_table.act_fname,actor_table.act_lname, movie_cast_table.role,
# MAGIC                  rank() OVER (ORDER BY CAST(movie_table.mov_time AS INTEGER)) AS rank_column
# MAGIC FROM Global_temp.movie_table
# MAGIC INNER JOIN Global_temp.movie_direction_table ON movie_table.mov_id = movie_direction_table.mov_id
# MAGIC INNER JOIN Global_temp.director_table ON director_table.dir_id = movie_direction_table.dir_id
# MAGIC INNER JOIN Global_temp.movie_cast_table ON movie_cast_table.mov_id = movie_table.mov_id
# MAGIC INNER JOIN Global_temp.actor_table ON actor_table.act_id = movie_cast_table.act_id
# MAGIC )
# MAGIC WHERE rank_column = 1;
# MAGIC --WHERE movie_table.mov_time = (SELECT MIN(cast(movie_table.mov_time AS INTEGER))FROM Global_temp.movie_table);

# COMMAND ----------

# DBTITLE 1,CTE
# MAGIC %sql
# MAGIC -- 7) solution -3
# MAGIC WITH movie_time_table AS(
# MAGIC SELECT movie_table.mov_title,movie_table.mov_time,movie_table.mov_year,director_table.dir_fname,director_table.dir_lname,
# MAGIC                  actor_table.act_fname,actor_table.act_lname, movie_cast_table.role,
# MAGIC                  rank() OVER (ORDER BY CAST(movie_table.mov_time AS INTEGER)) AS rank_column
# MAGIC FROM Global_temp.movie_table
# MAGIC INNER JOIN Global_temp.movie_direction_table ON movie_table.mov_id = movie_direction_table.mov_id
# MAGIC INNER JOIN Global_temp.director_table ON director_table.dir_id = movie_direction_table.dir_id
# MAGIC INNER JOIN Global_temp.movie_cast_table ON movie_cast_table.mov_id = movie_table.mov_id
# MAGIC INNER JOIN Global_temp.actor_table ON actor_table.act_id = movie_cast_table.act_id
# MAGIC ),
# MAGIC OUTPUT_TABLE AS(
# MAGIC SELECT * FROM movie_time_table
# MAGIC WHERE rank_column =1
# MAGIC )
# MAGIC SELECT * FROM OUTPUT_TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 9.From the following table, write a SQL query to find those movies that have at least one rating and received the most stars. Sort the result-set on movie title. Return movie title and maximum review stars.
# MAGIC SELECT * FROM
# MAGIC (SELECT movie_table.mov_title,max(rating_table.rev_stars) AS max_stars
# MAGIC FROM Global_temp.movie_table
# MAGIC INNER JOIN Global_temp.rating_table ON movie_table.mov_id = rating_table.mov_id
# MAGIC GROUP BY movie_table.mov_title)
# MAGIC WHERE max_stars IS NOT NULL
# MAGIC ORDER BY max_stars DESC;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 11. From the following table, write a SQL query to find movies in which one or more actors have acted in more than one film. Return movie title, actor first and last name, and the role.
# MAGIC SELECT * FROM
# MAGIC (SELECT ACT.act_fname,ACT.act_lname,COUNT(MOV.mov_title) AS CNT--,MCT.role,MOV.mov_title
# MAGIC FROM Global_temp.actor_table AS ACT
# MAGIC INNER JOIN global_temp.movie_cast_table AS MCT
# MAGIC ON MCT.act_id = ACT.act_id
# MAGIC INNER JOIN Global_temp.movie_table AS MOV
# MAGIC ON MOV.mov_id = MCT.mov_id
# MAGIC GROUP BY ACT.act_fname,ACT.act_lname)
# MAGIC WHERE CNT >1;

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH RE_ACT_TABLE AS (
# MAGIC SELECT ACT.act_fname,ACT.act_lname,COUNT(MOV.mov_title) AS CNT,ACT.act_id
# MAGIC FROM Global_temp.actor_table AS ACT
# MAGIC INNER JOIN global_temp.movie_cast_table AS MCT
# MAGIC ON MCT.act_id = ACT.act_id
# MAGIC INNER JOIN Global_temp.movie_table AS MOV
# MAGIC ON MOV.mov_id = MCT.mov_id
# MAGIC GROUP BY ACT.act_fname,ACT.act_lname,ACT.act_id
# MAGIC HAVING COUNT(MOV.mov_title)>1
# MAGIC )
# MAGIC SELECT * FROM RE_ACT_TABLE
# MAGIC INNER JOIN global_temp.movie_cast_table AS MCT
# MAGIC ON MCT.act_id = RE_ACT_TABLE.act_id
# MAGIC INNER JOIN  Global_temp.movie_table AS MOV
# MAGIC ON MOV.mov_id = MCT.mov_id
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM
# MAGIC (SELECT
# MAGIC COUNT(ACT.act_id) OVER (Partition BY ACT.act_id) AS CNT,ACT.act_fname,ACT.act_lname,MCT.role,MOV.mov_title
# MAGIC FROM Global_temp.actor_table AS ACT
# MAGIC INNER JOIN global_temp.movie_cast_table AS MCT
# MAGIC ON MCT.act_id = ACT.act_id
# MAGIC INNER JOIN Global_temp.movie_table AS MOV
# MAGIC ON MOV.mov_id = MCT.mov_id)
# MAGIC WHERE CNT >1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 12. From the following tables, write a SQL query to find the actor whose first name is 'Claire' and last name is 'Danes'. Return director first name, last name, movie title, actor first name and last name, role.
# MAGIC
# MAGIC SELECT director_table.dir_fname,director_table.dir_lname,movie_table.mov_title,actor_table.act_fname,actor_table.act_lname
# MAGIC FROM Global_temp.actor_table
# MAGIC INNER JOIN Global_temp.movie_cast_table ON movie_cast_table.act_id = actor_table.act_id
# MAGIC INNER JOIN Global_temp.movie_direction_table ON movie_direction_table.mov_id = movie_cast_table.mov_id
# MAGIC INNER JOIN Global_temp.director_table ON director_table.dir_id = movie_direction_table.dir_id
# MAGIC INNER JOIN Global_temp.movie_table ON movie_table.mov_id = movie_direction_table.mov_id
# MAGIC WHERE actor_table.act_fname = 'Claire' AND actor_table.act_lname = 'Danes';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 18. From the following table, write a SQL query to find for actors whose films have been directed by them. Return actor first name, last name, movie title and role.
# MAGIC SELECT movie_table.mov_title,actor_table.act_fname,actor_table.act_lname
# MAGIC FROM Global_temp.actor_table
# MAGIC INNER JOIN Global_temp.movie_cast_table ON movie_cast_table.act_id = actor_table.act_id
# MAGIC INNER JOIN Global_temp.movie_direction_table ON movie_direction_table.mov_id = movie_cast_table.mov_id
# MAGIC INNER JOIN Global_temp.director_table ON director_table.dir_id = movie_direction_table.dir_id
# MAGIC INNER JOIN Global_temp.movie_table ON movie_table.mov_id = movie_direction_table.mov_id
# MAGIC WHERE director_table.dir_fname = actor_table.act_fname
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 21. From the following tables, write a SQL query to find the highest-rated movies. Return movie title, movie year, review stars and releasing country.
# MAGIC WITH CTE AS(
# MAGIC SELECT movie_table.mov_title,movie_table.mov_year,rating_table.rev_stars,movie_table.mov_rel_country,
# MAGIC                         row_number() OVER (ORDER BY CAST(rating_table.rev_stars AS FLOAT) DESC) AS high_rated_movies
# MAGIC FROM Global_temp.movie_table
# MAGIC INNER JOIN Global_temp.rating_table ON movie_table.mov_id = rating_table.mov_id
# MAGIC )
# MAGIC SELECT * FROM 
# MAGIC CTE
# MAGIC WHERE high_rated_movies =1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE AS(
# MAGIC SELECT --movie_table.mov_title,
# MAGIC max(CAST(rating_table.rev_stars AS FLOAT)) AS max_stars
# MAGIC FROM Global_temp.movie_table
# MAGIC INNER JOIN Global_temp.rating_table ON movie_table.mov_id = rating_table.mov_id
# MAGIC --GROUP BY movie_table.mov_title
# MAGIC )
# MAGIC SELECT * FROM
# MAGIC CTE
# MAGIC --WHERE max_stars =1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 22. From the following tables, write a SQL query to find the highest-rated ‘Mystery Movies’. Return the title, year, and rating.
# MAGIC
# MAGIC SELECT movie_table.mov_title,movie_table.mov_year,rating_table.rev_stars,rating_table.num_o_ratings
# MAGIC FROM global_temp.movie_table
# MAGIC INNER JOIN Global_temp.rating_table ON rating_table.mov_id = movie_table.mov_id
# MAGIC INNER JOIN Global_temp.movie_genre_table ON movie_genre_table.mov_id = movie_table.mov_id
# MAGIC INNER JOIN Global_temp.genre_table ON genre_table.gen_id = movie_genre_table.gen_id
# MAGIC WHERE genre_table.gen_title = 'Mystery';
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 23. From the following tables, write a SQL query to find the years when most of the ‘Mystery Movies’ produced. Count the number of generic title and compute their average rating. Group the result set on movie release year, generic title. Return movie year, generic title, number of generic title and average rating.
# MAGIC WITH Mystery_CTE AS(
# MAGIC SELECT genre_table.gen_title,movie_table.mov_year,rating_table.rev_stars,genre_table.gen_id
# MAGIC FROM global_temp.movie_table
# MAGIC INNER JOIN Global_temp.rating_table ON rating_table.mov_id = movie_table.mov_id
# MAGIC INNER JOIN Global_temp.movie_genre_table ON movie_genre_table.mov_id = movie_table.mov_id
# MAGIC INNER JOIN Global_temp.genre_table ON genre_table.gen_id = movie_genre_table.gen_id
# MAGIC WHERE genre_table.gen_title = 'Mystery'
# MAGIC )
# MAGIC SELECT COUNT(gen_title),AVG(rev_stars),gen_title,mov_year
# MAGIC FROM Mystery_CTE
# MAGIC GROUP BY gen_title,mov_year
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH Mystery_CTE AS(
# MAGIC SELECT genre_table.gen_title,movie_table.mov_year,rating_table.rev_stars,genre_table.gen_id
# MAGIC FROM global_temp.movie_table
# MAGIC INNER JOIN Global_temp.rating_table ON rating_table.mov_id = movie_table.mov_id
# MAGIC INNER JOIN Global_temp.movie_genre_table ON movie_genre_table.mov_id = movie_table.mov_id
# MAGIC INNER JOIN Global_temp.genre_table ON genre_table.gen_id = movie_genre_table.gen_id
# MAGIC WHERE genre_table.gen_title = 'Mystery'
# MAGIC )
# MAGIC SELECT COUNT(gen_title) OVER(PARTITION BY mov_year,gen_title) AS CNT,AVG(rev_stars) OVER(PARTITION BY mov_year,gen_title) AS AVERAGE,gen_title,mov_year
# MAGIC FROM Mystery_CTE
# MAGIC -- it will not reduce the number of records ,if we have 3 records for the genre then all the 3 records will be displayed with same avg values 
# MAGIC --Then use Group By for one record. 