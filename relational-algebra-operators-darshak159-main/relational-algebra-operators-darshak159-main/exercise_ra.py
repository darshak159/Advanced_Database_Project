import ra_operator as RA;
import os
import csv

# Data source: https://relational.fit.cvut.cz/dataset/IMDb
# Information courtesy of IMDb (http://www.imdb.com). Used with permission.
# Notice: The data can only be used for personal and non-commercial use and must not
# be altered/republished/resold/repurposed to create any kind of online/offline
# database of movie information (except for individual personal use).


import os

print('basename:    ', os.path.basename(__file__))
print('dirname:     ', os.path.dirname(__file__))


filepath = os.path.dirname(__file__)+ '/data/IMDb_sample'  
print(filepath)

def open_file(filename):
    with open(filename, 'r', newline = '') as f:
        reader = csv.reader(f, delimiter='\t')
        data = list(reader)
        return data[1:]

    
def main():

    # Creating another table with actors for testing purposes
    extra_actors = [['109100', 'Renata', 'Dividino', 'F'], ['481290', 'Burnell', 'Tucker', 'M'], ['10963', 'Chris', 'Anastasio', 'M']]

    # create a list of all files in that directory that end with "*.csv":
    actors = open_file(filepath + '/actors.csv')
    directors_genres = open_file(filepath + '/directors_genres.csv')
    directors = open_file(filepath + '/directors.csv')
    movies_directors = open_file(filepath + '/movies_directors.csv')
    movies_genres = open_file(filepath + '/movies_genres.csv')
    movies = open_file(filepath + '/movies.csv')
    roles = open_file(filepath + '/roles.csv')

    # 1- Perform the union between the actors and extra_actors relation
    # This operation should returns all of the rows of actor and extra_actors unioned in to a single list. 
    union_result = RA.union(actors, extra_actors)
    print("1- Union between actors and extra_actors:")
    print(union_result)
    print()

    # 2- Show all tuples from actors that first name is Chris
    selection_result = RA.selection(actors, 1, 'Chris', '==')
    print("2- Tuples from actors with first name Chris:")
    print(selection_result)
    print()

    # 3 - Show all tuples from movies that were made after 1998
    selection_result_movies = RA.selection(movies, 2, 1998, '>')
    print("3- Tuples from movies made after 1998:")
    print(selection_result_movies)
    print()

    # 4 - Show all tuples from actors that are female AND id is bigger than 200000
    selection_result_female_actors = RA.selection(RA.selection(actors, 3, 'F', '=='), 0, 200000, '>')
    print("4- Female actors with id greater than 200000:")
    print(selection_result_female_actors)
    print()

    # 5 - Find the last name of all actors named Chris
    projection_result_chris_last_names = RA.project(RA.selection(actors, 1, 'Chris', '=='), [2])
    print("5- Last names of actors named Chris:")
    print(projection_result_chris_last_names)
    print()

    # 6 - Find the names of the movies that were made after 1998
    projection_result_movies_after_1998 = RA.project(RA.selection(movies, 2, 1998, '>'), [1])
    print("6- Names of movies made after 1998:")
    print(projection_result_movies_after_1998)
    print()

    # 7 - Find the name of actors that are female AND id is bigger than 200000
    projection_result_female_actors_after_200000 = RA.project(RA.selection(RA.selection(actors, 3, 'F', '=='), 0, 200000, '>'), [1, 2])
    print("7- Names of female actors with id greater than 200000:")
    print(projection_result_female_actors_after_200000)
    print()

    # 8 - Take the cross product of actors and movies
    cross_product_result = RA.crossproduct(actors, movies)
    print("8- Cross product of actors and movies:")
    print(cross_product_result)
    print()

    # 9 - Print the name of actors and the name of movies they have had a role in it.
    joined_result = RA.crossproduct(roles, RA.crossproduct(actors, movies))
    print("9- Name of actors and names of movies they had a role in:")
    print(joined_result)
    print()
 
if __name__ == "__main__":
 main()