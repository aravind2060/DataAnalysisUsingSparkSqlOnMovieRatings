import csv
import random
import os

def generate_movie(movie_id):
    """Generate movie data with given movie ID."""
    title = f"Movie {movie_id}"
    genre = random.choice(["Action", "Comedy", "Drama", "Thriller", "Sci-Fi"])
    return [movie_id, title, genre]

def generate_rating(user_id, movie_id):
    """Generate rating data with given user ID and movie ID."""
    rating = random.randint(1, 5)
    return [user_id, movie_id, rating]

def generate_movies_data(num_movies):
    """Generate movies data for the given number of movies."""
    return [generate_movie(movie_id) for movie_id in range(1, num_movies + 1)]

def generate_ratings_data(num_users, num_movies, max_ratings_per_user):
    """Generate ratings data for the given number of users, movies, and maximum ratings per user."""
    ratings_data = []
    for user_id in range(1, num_users + 1):
        num_ratings = random.randint(1, max_ratings_per_user)
        rated_movies = random.sample(range(1, num_movies + 1), num_ratings)
        for movie_id in rated_movies:
            ratings_data.append(generate_rating(user_id, movie_id))
    return ratings_data

def write_to_csv(file_path, header, data):
    """Write data to a CSV file with the given file path and header."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)  # Create directory if it doesn't exist
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(data)

def main():
    num_movies = 100
    num_users = 50
    max_ratings_per_user = 20

    movies_data = generate_movies_data(num_movies)
    ratings_data = generate_ratings_data(num_users, num_movies, max_ratings_per_user)

    movies_header = ["movieId", "title", "genre"]
    ratings_header = ["userId", "movieId", "rating"]

    write_to_csv("../data/input/movies.csv", movies_header, movies_data)
    write_to_csv("../data/input/ratings.csv", ratings_header, ratings_data)

if __name__ == "__main__":
    main()