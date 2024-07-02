from flask import Flask, jsonify, request
import pymysql
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

connection = pymysql.connect(
    host='localhost',
    user='root',
    password='',
    database='test',
    cursorclass=pymysql.cursors.DictCursor
)

# Retrieve movies
@app.route('/movies', methods=['GET'])
def get_movies():
    query_params = request.args
    if 'name' in query_params:
        movies = get_movies_by_name(query_params['name'])
    elif 'genre' in query_params:
        movies = get_movies_by_genre(query_params['genre'])
    elif 'rating' in query_params:
        movies = get_movies_by_rating(float(query_params['rating']))
    
    else:
        return jsonify({"error": "Invalid query parameters"}), 400
    
    # Fetch cast details for each movie
    for movie in movies:
        movie['cast'] = get_cast_for_movie(movie['movie_id'])
    
    return jsonify(movies)

# Get movies by name
def get_movies_by_name(name):
    with connection.cursor() as cursor:
        sql = "SELECT Movies.*, AVG(Reviews.rating) AS rating FROM Movies LEFT JOIN Reviews ON Movies.movie_id = Reviews.movie_id WHERE title LIKE %s GROUP BY Movies.movie_id"
        cursor.execute(sql, ('%' + name + '%',))
        return cursor.fetchall()

# Get movies by genre
def get_movies_by_genre(genre):
    with connection.cursor() as cursor:
        sql = "SELECT Movies.*, AVG(Reviews.rating) AS rating FROM Movies LEFT JOIN Reviews ON Movies.movie_id = Reviews.movie_id WHERE genre = %s GROUP BY Movies.movie_id"
        cursor.execute(sql, (genre,))
        return cursor.fetchall()

# Get movies by rating
def get_movies_by_rating(rating):
    with connection.cursor() as cursor:
        sql = "SELECT Movies.*, AVG(Reviews.rating) AS rating FROM Movies LEFT JOIN Reviews ON Movies.movie_id = Reviews.movie_id GROUP BY Movies.movie_id HAVING rating > %s"
        cursor.execute(sql, (rating,))
        return cursor.fetchall()
    


# Get cast for a movie
def get_cast_for_movie(movie_id):
    with connection.cursor() as cursor:
        sql = "SELECT Actors.name FROM Actors INNER JOIN MovieActors ON Actors.actor_id = MovieActors.actor_id WHERE MovieActors.movie_id = %s"
        cursor.execute(sql, (movie_id,))
        cast = [actor['name'] for actor in cursor.fetchall()]
        return cast

@app.route('/movies', methods=['POST'])
def insert_movie():
    movie_data = request.json
    # Assuming movie_data contains all required fields for inserting a movie
    # Implement your validation logic here if needed
    with connection.cursor() as cursor:
        sql = "INSERT INTO Movies (title, release_year, genre, image_link, director, video_link, runtime_minutes) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(sql, (movie_data['title'], movie_data['release_year'], movie_data['genre'], movie_data['image_link'], movie_data['director'], movie_data['video_link'], movie_data['runtime_minutes']))
        connection.commit()
    return jsonify({"message": "Movie inserted successfully"}), 201

@app.route('/movies/<string:movie_name>', methods=['POST'])
def update_movie(movie_name):
    movie_data = request.json
    with connection.cursor() as cursor:
        sql = "UPDATE Movies SET release_year=%s, genre=%s, image_link=%s, director=%s, video_link=%s, runtime_minutes=%s WHERE title=%s"
        cursor.execute(sql, (movie_data['release_year'], movie_data['genre'], movie_data['image_link'], movie_data['director'], movie_data['video_link'], movie_data['runtime_minutes'], movie_name))
        connection.commit()
    return jsonify({"message": "Movie updated successfully"})

# Delete a movie
@app.route('/movies/delete/<string:movie_name>', methods=['DELETE'])
def delete_movie(movie_name):
    with connection.cursor() as cursor:
        sql = "DELETE FROM Movies WHERE title=%s"
        cursor.execute(sql, (movie_name,))
        connection.commit()
    return jsonify({"message": "Movie deleted successfully"})

# Add search by language endpoint with cast details
# Add search by language endpoint with cast details
@app.route('/movies/language/<string:language>', methods=['GET'])
def get_movies_by_language(language):
    with connection.cursor() as cursor:
        sql = """
            SELECT 
                Movies.*, 
                AVG(Reviews.rating) AS rating,
                GROUP_CONCAT(Actors.name) AS cast
            FROM 
                Movies 
                LEFT JOIN Reviews ON Movies.movie_id = Reviews.movie_id 
                LEFT JOIN MovieActors ON Movies.movie_id = MovieActors.movie_id
                LEFT JOIN Actors ON MovieActors.actor_id = Actors.actor_id
            WHERE 
                language = %s 
            GROUP BY 
                Movies.movie_id
        """
        cursor.execute(sql, (language,))
        movies = cursor.fetchall()

        # Convert the cast string to a list
        for movie in movies:
            movie['cast'] = movie['cast'].split(',') if movie['cast'] else []

        return jsonify(movies)



    

if __name__ == '__main__':
    app.run(debug=True)
