CREATE KEYSPACE code500 WITH REPLICATION = {
'class': 'SimpleStrategy', 'replication_factor': 2 };


CREATE table credits (cast text, crew text, id int PRIMARY KEY);
CREATE table ratings (userid int, movieId PRIMARY KEY, ratings double, timestamp int)
CREATE table keywords (id int PRIMARY KEY, keywords text)
CREATE table movies 
(
adult boolean,
belongs_to_collection text,
budget int, 
genres text,
homepage text,
id int ,
imdb_id text,
original_language text,
original_title text,
overview text,
popularity double,
poster_path text,
production_companies text,
production_countries text,
release_date date,
revenue int,
runtime int,
spoken_languages text,
status text,
tagline text,
title text,
video boolean,
vote_average double,
vote_count int, 
PRIMARY KEY(id, imdb_id)
 )
