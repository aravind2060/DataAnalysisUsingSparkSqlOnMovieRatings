from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Movie Ratings Analysis") \
    .getOrCreate()

def set_database():
    databases = spark.catalog.listDatabases()
    database_names = [db.name for db in databases]
    if "movie_ratings" not in database_names:
        spark.sql("CREATE DATABASE IF NOT EXISTS movie_ratings")
    spark.sql("USE movie_ratings")

    
def load_data_if_not_exists():
    set_database()
    tables = spark.sql("SHOW TABLES IN movie_ratings")
    tables_list = [row["tableName"] for row in tables.collect()]

    if "movies" not in tables_list:
        # Load movies data from CSV
        movies_df = spark.read.csv("/data/input/movies.csv", header=True)
        movies_df.write.saveAsTable("movies", mode="overwrite")

    if "ratings" not in tables_list:
        # Load ratings data from CSV
        ratings_df = spark.read.csv("/data/input/ratings.csv", header=True)
        ratings_df.write.saveAsTable("ratings", mode="overwrite")

def count_movies_and_ratings():
    set_database()
    movies_count = spark.sql("SELECT COUNT(*) AS total_movies FROM movies")
    ratings_count = spark.sql("SELECT COUNT(*) AS total_ratings FROM ratings")

    movies_count.write.csv('/data/output/total_movies.csv', header=True)
    ratings_count.write.csv('/data/output/total_ratings.csv', header=True)

def average_rating_per_movie():
    set_database()
    avg_rating_per_movie = spark.sql("""
    SELECT movieId, AVG(rating) AS avg_rating
    FROM ratings
    GROUP BY movieId
    """)

    avg_rating_per_movie.write.csv('/data/output/avg_rating_per_movie.csv', header=True)

def top_rated_movies():
    set_database()
    top_rated_movies = spark.sql("""
    SELECT m.movieId, m.title, AVG(r.rating) AS avg_rating
    FROM movies m
    JOIN ratings r ON m.movieId = r.movieId
    GROUP BY m.movieId, m.title
    ORDER BY avg_rating DESC
    LIMIT 10
    """)

    top_rated_movies.write.csv('/data/output/top_rated_movies.csv', header=True)


def rating_rank_per_genre():
    set_database()
    rating_rank_per_genre = spark.sql("""
    SELECT movieId, title, genre, RANK() OVER (PARTITION BY genre ORDER BY avg_rating DESC) AS genre_rank
    FROM (
        SELECT m.movieId, m.title, m.genre, AVG(r.rating) AS avg_rating
        FROM movies m
        JOIN ratings r ON m.movieId = r.movieId
        GROUP BY m.movieId, m.title, m.genre
    ) AS genre_ratings
    """)

    rating_rank_per_genre.write.csv('/data/output/rating_rank_per_genre.csv', header=True)

def top_n_recommendations(given_userId, N):
    set_database()
    top_n_recommendations = spark.sql(f"""
    WITH user_rated_movies AS (
        SELECT movieId
        FROM ratings
        WHERE userId = {given_userId}
    ),
    avg_movie_ratings AS (
        SELECT movieId, AVG(rating) AS avg_rating
        FROM ratings
        WHERE movieId NOT IN (SELECT movieId FROM user_rated_movies)
        GROUP BY movieId
    )
    SELECT m.movieId, m.title, r.avg_rating
    FROM movies m
    JOIN avg_movie_ratings r ON m.movieId = r.movieId
    ORDER BY r.avg_rating DESC
    LIMIT {N}
    """)

    top_n_recommendations.write.csv(f'/data/output/top_n_recommendations_{given_userId}.csv', header=True)


def diverse_users(threshold):
    set_database()
    diverse_users = spark.sql(f"""
    SELECT userId, COUNT(DISTINCT genre) AS genre_count
    FROM ratings r
    JOIN movies m ON r.movieId = m.movieId
    GROUP BY userId
    HAVING genre_count >= {threshold}
    """)

    diverse_users.write.csv('/data/output/diverse_users.csv', header=True)

def average_rating_per_genre():
    set_database()
    avg_rating_per_genre = spark.sql("""
    SELECT genre, AVG(r.rating) AS avg_genre_rating
    FROM movies m
    JOIN ratings r ON m.movieId = r.movieId
    GROUP BY genre
    ORDER BY avg_genre_rating DESC
    """)

    avg_rating_per_genre.write.csv('/data/output/avg_rating_per_genre.csv', header=True)

# Example usage
if __name__ == "__main__":
    load_data_if_not_exists()  # Load data into tables only if they don't exist
    count_movies_and_ratings()
    average_rating_per_movie()
    top_rated_movies()
    rating_rank_per_genre()
    top_n_recommendations(1, 10)  # assuming userId 1 and top 10 movies
    diverse_users(5)  # assuming threshold of 5 genres
    average_rating_per_genre()
