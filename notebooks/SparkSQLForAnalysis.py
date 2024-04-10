from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Movie Ratings Analysis") \
    .getOrCreate()

def set_database():
    spark.sql("USE movie_ratings")

def count_movies_and_ratings():
    set_database()
    movies_count = spark.sql("SELECT COUNT(*) AS total_movies FROM movies")
    ratings_count = spark.sql("SELECT COUNT(*) AS total_ratings FROM ratings")

    movies_count.write.csv('/data/output/total_movies.csv', header=True)
    ratings_count.write.csv('/data/output/total_ratings.csv', header=True)

def average_rating_per_movie():
    set_database()
    avg_rating_per_movie = spark.sql("""
    SELECT movie_id, AVG(rating) AS avg_rating
    FROM ratings
    GROUP BY movie_id
    """)

    avg_rating_per_movie.write.csv('/data/output/avg_rating_per_movie.csv', header=True)

def top_rated_movies():
    set_database()
    top_rated_movies = spark.sql("""
    SELECT m.movie_id, m.title, AVG(r.rating) AS avg_rating
    FROM movies m
    JOIN ratings r ON m.movie_id = r.movie_id
    GROUP BY m.movie_id, m.title
    ORDER BY avg_rating DESC
    LIMIT 10
    """)

    top_rated_movies.write.csv('/data/output/top_rated_movies.csv', header=True)

def rating_rank_per_genre():
    set_database()
    rating_rank_per_genre = spark.sql("""
    SELECT movie_id, title, genre, RANK() OVER (PARTITION BY genre ORDER BY avg_rating DESC) AS genre_rank
    FROM (
        SELECT m.movie_id, m.title, m.genre, AVG(r.rating) AS avg_rating
        FROM movies m
        JOIN ratings r ON m.movie_id = r.movie_id
        GROUP BY m.movie_id, m.title, m.genre
    ) AS genre_ratings
    """)

    rating_rank_per_genre.write.csv('/data/output/rating_rank_per_genre.csv', header=True)

def top_n_recommendations(given_user_id, N):
    set_database()
    top_n_recommendations = spark.sql(f"""
    WITH user_rated_movies AS (
        SELECT movie_id
        FROM ratings
        WHERE user_id = {given_user_id}
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
    LIMIT {N}
    """)

    top_n_recommendations.write.csv(f'/data/output/top_n_recommendations_{given_user_id}.csv', header=True)

def diverse_users(threshold):
    set_database()
    diverse_users = spark.sql(f"""
    SELECT user_id, COUNT(DISTINCT genre) AS genre_count
    FROM ratings r
    JOIN movies m ON r.movie_id = m.movie_id
    GROUP BY user_id
    HAVING genre_count >= {threshold}
    """)

    diverse_users.write.csv('/data/output/diverse_users.csv', header=True)

def average_rating_per_genre():
    set_database()
    avg_rating_per_genre = spark.sql("""
    SELECT genre, AVG(r.rating) AS avg_genre_rating
    FROM movies m
    JOIN ratings r ON m.movie_id = r.movie_id
    GROUP BY genre
    ORDER BY avg_genre_rating DESC
    """)

    avg_rating_per_genre.write.csv('/data/output/avg_rating_per_genre.csv', header=True)

# Example usage
if __name__ == "__main__":
    count_movies_and_ratings()
    average_rating_per_movie()
    top_rated_movies()
    rating_rank_per_genre()
    top_n_recommendations(1, 10)  # assuming user_id 1 and top 10 movies
    diverse_users(5)  # assuming threshold of 5 genres
    average_rating_per_genre()
