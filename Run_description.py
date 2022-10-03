import os

from imdb_code import get_movie_descriptions_by_actor_soup
from imdb_helper_functions import helper_get_soup
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

    for url in _urls:
       soup = helper_get_soup(url)
       get_movie_descriptions_by_actor_soup(soup, 8, os.getcwd() + '/Descriptions')

    t2 = time.perf_counter()
    print(f'Finished in {t2 - t1} seconds')
