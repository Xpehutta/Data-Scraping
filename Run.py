from imdb_code import get_movie_distance
import time

if __name__ == '__main__':

    t1 = time.perf_counter()

    _urls = ['https://www.imdb.com/name/nm0425005/',
        'https://www.imdb.com/name/nm1165110/',
        'https://www.imdb.com/name/nm0000375/',
        'https://www.imdb.com/name/nm0474774/',
        'https://www.imdb.com/name/nm0000329/',
        'https://www.imdb.com/name/nm0177896/',
        'https://www.imdb.com/name/nm0001191/',
        'https://www.imdb.com/name/nm0424060/',
        'https://www.imdb.com/name/nm0005527/',
        'https://www.imdb.com/name/nm0262635/']

    it = 1

    for i in _urls[:-1]:
        for j in _urls[it:]:
            print(get_movie_distance(actor_start_url=i
                       , actor_end_url=j
                       , num_of_actors_limit=5
                       , num_of_movies_limit=5
                       , clean_result_file=False
                       , parallel=8))
        it += 1

    t2 = time.perf_counter()
    print(f'Finished in {t2 - t1} seconds')