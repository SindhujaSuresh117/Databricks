Sample Database: movie

This documentation will give some basic idea about a movie related database. This will help the user to think about the question comes to their mind and how they troubleshoot it.

Sample Database description:

The sample database represents some of the data storage and retrieval about a movie related industry. Most of the people loves to watch movie, and for all of them we are providing a sample information about the movie related questions coming to their mind. This design of database will make it easier to the movie lovers to know the curiocities about the movies

List of tables in the movie database:

actor
genres
director
movie
movie_genres
movie_direction
reviewer
rating
movie_cast

Description of tables:

actor:

act_id – this is a unique ID for each actor
act_fname – this is the first name of each actor
act_lname – this is the last name of each actor
act_gender – this is the gender of each actor
genres:

gen_id – this is a unique ID for each genres
gen_title – this is the description of the genres
director:

dir_id – this is a unique ID for each director
dir_fname – this is the first name of the director
dir_lname – this is the last name of the director

movie:

mov_id – this is the unique ID for each movie
mov_title – this column represents the name of the movie
mov_year – this is the year of making the movie
mov_time – duration of the movie i.e. how long it was running
mov_lang – the language in which movie was casted
mov_dt_rel – this is the release date of the movie
mov_rel_country – this is the name of the country(s) where the movie was released

movie_genres:

mov_id – this is the ID of the movie, which is referencing the mov_id column of the table movie
gen_id – this is the ID of each genres, which is referencing the gen_id column of the table genres

movie_direction:

dir_id – this is the ID for each director, which is referencing the dir_id column of the table director
mov_id – this is the ID of the movie, which is referencing the mov_id column of the table movie

reviewer:

rev_id – this is the unique ID for each reviewer
rev_name – this is the name of the reviewer

rating:

mov_id –this is the ID of the movie, which is referencing the mov_id column of the table movie
rev_id – this is the ID of the reviewer, which is referencing the rev_id column of the table reviewer
rev_stars – this is indicates how many stars a reviewer rated for a review of a movie
num_o_rating – this indicates how many ratings a movie achieved till date

movie_cast:

act_id – this is ID of actor, which is referencing the act_id column of actor table
mov_id – this is the ID of the movie, which is referencing the mov_id column of the table movie
role – this is the name of a character in the movie, an actor acted for that character
