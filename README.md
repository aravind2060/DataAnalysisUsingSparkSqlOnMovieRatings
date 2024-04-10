# Spark SQL Analysis on Movie Ratings

## Overview

This project involves analyzing a dataset containing movie ratings using Spark SQL. The objective is to perform various data processing and analysis tasks related to movie recommendations.

To start the Spark cluster using Docker Compose and access the Spark Master UI using Docker CLI, you can follow these steps:

1. **Start the Spark cluster**: Open a terminal, navigate to the directory containing `docker-compose.yml`, and run the following command:

```bash
docker-compose up -d
```

2. **Access Spark Master UI**: Once the containers are up and running, you can access the Spark Master UI using Docker CLI. Open a terminal and run:

```bash
docker exec -it my-spark-master bash
```

## Setup and Initialization

1. **Start Spark Session**:
   Ensure that your Spark cluster is up and running, and start a Spark session or Spark SQL CLI.

   ``` CMD
   spark-sql
   ```

3. **Create the Database**:
   ```sql
   CREATE DATABASE movie_ratings;
   USE movie_ratings;
   ```

4. **Create Tables**:
   Assuming you have CSV files for movies and ratings, execute the following commands to create the respective tables:

   ```sql
   CREATE TABLE movies (
       movie_id INT,
       title STRING,
       genre STRING
   )
   USING csv
   OPTIONS (path '/data/input/movies.csv', header 'true');

   CREATE TABLE ratings (
       user_id INT,
       movie_id INT,
       rating FLOAT
   )
   USING csv
   OPTIONS (path '/data/input/ratings.csv', header 'true');
   ```

## Data Analysis

### Total Number of Movies and Ratings

Calculate the total number of movies and ratings in the dataset.

```sql
SELECT COUNT(*) AS total_movies FROM movies;
SELECT COUNT(*) AS total_ratings FROM ratings;
```

### Average Rating for Each Movie

Find the average rating for each movie.

```sql
SELECT movie_id, AVG(rating) AS avg_rating
FROM ratings
GROUP BY movie_id;
```

### Top-Rated Movies

Identify the movies with the highest average ratings.

```sql
SELECT m.movie_id, m.title, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id, m.title
ORDER BY avg_rating DESC
LIMIT 10;
```

### Movies Rating Rank per Genre

Use window functions to compute the ranking of movies by ratings within each genre.

```sql
SELECT movie_id, title, genre, RANK() OVER (PARTITION BY genre ORDER BY avg_rating DESC) AS genre_rank
FROM (
    SELECT m.movie_id, m.title, m.genre, AVG(r.rating) AS avg_rating
    FROM movies m
    JOIN ratings r ON m.movie_id = r.movie_id
    GROUP BY m.movie_id, m.title, m.genre
) AS genre_ratings;
```

### Top N Movie Recommendations for a Given User

Generate top movie recommendations for a specific user.

```sql
-- Replace [given_user_id] and [N] with appropriate values
WITH user_rated_movies AS (
    SELECT movie_id
    FROM ratings
    WHERE user_id = [given_user_id]
),
avg_movie_ratings AS (
    SELECT movie_id, AVG(rating) AS avg_rating
    FROM ratings
    WHERE movie_id NOT IN (SELECT movie_id FROM user_rated_movies)
    GROUP BY movie_id
)
SELECT m.movie_id, m.title, r.avg_rating
FROM movies m
JOIN avg_movie_ratings r ON m.movie_id = r.movie_id
ORDER BY r.avg_rating DESC
LIMIT [N];
```

### Users With Diverse Movie Ratings

Identify users who have rated movies across a wide variety of genres.

```sql
-- Replace [threshold] with the minimum number of genres
SELECT user_id, COUNT(DISTINCT genre) AS genre_count
FROM ratings r
JOIN movies m ON r.movie_id = m.movie_id
GROUP BY user_id
HAVING genre_count >= [threshold];
```

### Average Rating per Genre

Calculate and identify the genres with the highest average ratings.

```sql
SELECT genre, AVG(r.rating) AS avg_genre_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY genre
ORDER BY avg_genre_rating DESC;
```
