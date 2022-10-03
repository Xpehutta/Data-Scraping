from imdb_helper_functions import *
import multiprocessing
import time
from bs4 import BeautifulSoup


def get_movies_by_actor_soup(actor_page_soup, num_of_movies_limit=None):
    _lst = helper_get_info_actor(actor_page_soup)

    if num_of_movies_limit is not None:
        _lst = _lst[:num_of_movies_limit]

    return _lst


def get_actors_by_movie_soup(cast_page_soup, num_of_actors_limit=None):
    _lst = helper_get_info_movie(cast_page_soup)

    if num_of_actors_limit is not None:
        _lst = _lst[:num_of_actors_limit]

    return _lst


def get_movie_distance(actor_start_url
                       , actor_end_url
                       , num_of_actors_limit=None
                       , num_of_movies_limit=None
                       , clean_result_file=False
                       , parallel=None
                       ):
    actor_start_url = helper_www_add(actor_start_url)
    actor_end_url = helper_www_add(actor_end_url)

    current_level = 1

    helper_file(file_name='actors', to_do='erase')
    helper_file(file_name='movies', to_do='erase')
    helper_file(file_name='actors_from_scan', to_do='erase')

    if clean_result_file:
        helper_file(file_name='results', to_do='erase', print_logs=False)

    movie_urls = helper_url_collect(url=actor_start_url
                                    , func='actors'
                                    , file=True, to_do='append'
                                    , num_of_urls=num_of_movies_limit)

    print(f'************** LEVEL {current_level} **************\n')

    if parallel is not None:

        data = helper_url_collect_parallel(urls=movie_urls, func='movies', file=True, to_do='append',
                                           num_of_urls=num_of_actors_limit, parallel=parallel)

        helper_file(data=data, file_name='actors_from_scan', to_do='append', print_logs=False)

        if actor_end_url in data:
            print(f'{actor_end_url} was found on level {current_level}')
            st = actor_start_url + ';' + actor_end_url + ';' + str(current_level)
            res = [st]
            helper_file(data=res, file_name='results', to_do='append', print_logs=False)
            return current_level

    else:
        for url in movie_urls:

            data = helper_url_collect(url=url, func='movies'
                                      , file=True
                                      , to_do='append'
                                      , num_of_urls=num_of_actors_limit)

            helper_file(data=data, file_name='actors_from_scan', to_do='append', print_logs=False)

            if actor_end_url in data:
                print(f'{actor_end_url} was found on level {current_level}')
                st = actor_start_url + ';' + actor_end_url + ';' + str(current_level)
                res = [st]
                helper_file(data=res, file_name='results', to_do='append')
                return current_level
            else:
                print('Keep collecting data')

        t2 = time.perf_counter()

    actor_urls = helper_file(file_name='actors_from_scan', to_do='read', print_logs=False)
    actor_urls = list(set(actor_urls))

    actor_urls_scanned = helper_file(file_name='actors', to_do='read', print_logs=False)

    actor_urls = [i for i in actor_urls if i not in actor_urls_scanned]

    while True:

        current_level += 1

        print(f'************** LEVEL {current_level} **************\n')

        if current_level > 3:
            print(f'{actor_end_url} was found on level -1')
            st = actor_start_url + ';' + actor_end_url + ';' + '-1'
            res = [st]
            helper_file(data=res, file_name='results', to_do='append', print_logs=False)
            return -1

        helper_file(file_name='actors_from_scan', to_do='erase', print_logs=False)

        if parallel is not None:

            with multiprocessing.Pool(processes=parallel) as pool:

                data = [pool.apply_async(helper_url_collect, args=(url, 'actors', 10, True, 'append', None, True)) for url in
                        actor_urls]
                for r in data:
                    movie_urls_scanned = helper_file(file_name='movies', to_do='read', print_logs=False)
                    movie_urls = [i for i in r.get() if i not in movie_urls_scanned][:num_of_movies_limit]

                    with multiprocessing.Pool(processes=parallel) as pool:

                        data = [pool.apply_async(helper_url_collect,
                                                 args=(url, 'movies', 10, True, 'append', num_of_actors_limit, True))
                                for url in movie_urls]

                        for r in data:

                            helper_file(data=r.get(), file_name='actors_from_scan', to_do='append', print_logs=False)

                            if actor_end_url in r.get():
                                print(f'{actor_end_url} was found on level {current_level}')
                                st = actor_start_url + ';' + actor_end_url + ';' + str(current_level)
                                res = [st]
                                helper_file(data=res, file_name='results', to_do='append', print_logs=False)
                                return current_level

        else:
            for actor_url in actor_urls:

                movie_urls_scanned = helper_file(file_name='movies', to_do='read')
                movie_urls = helper_url_collect(url=actor_url, func='actors', file=True, to_do='append')

                movie_urls = [i for i in movie_urls if i not in movie_urls_scanned][:num_of_movies_limit]

                for url in movie_urls:

                    data = helper_url_collect(url=url
                                              , func='movies'
                                              , file=True
                                              , to_do='append'
                                              , num_of_urls=num_of_actors_limit)

                    helper_file(data=data, file_name='actors_from_scan', to_do='append')

                    if actor_end_url in data:
                        print(f'{actor_end_url} was found on level {current_level}')
                        st = actor_start_url + ';' + actor_end_url + ';' + str(current_level)
                        res = [st]
                        helper_file(res, 'results', to_do='append')
                        return current_level
                    else:
                        print('Keep collecting data')

        actor_urls = helper_file(file_name='actors_from_scan', to_do='read', print_logs=False)
        actor_urls = list(set(actor_urls))

        actor_urls_scanned = helper_file(file_name='actors', to_do='read', print_logs=False)

        actor_urls = [i for i in actor_urls if i not in actor_urls_scanned]


def get_movie_descriptions_by_actor_soup(actor_page_soup, parallel=None, directory=os.getcwd()):
    actor_name = actor_page_soup.find(class_="itemprop").text
    _result = []
    movie_url_list = helper_get_info_actor(actor_page_soup)

    if parallel is not None:
        urls = [i[1] for i in movie_url_list]
        with multiprocessing.Pool(processes=parallel) as pool:
            data = [pool.apply_async(helper_get_soup, args = (url, 10, True)) for url in urls]

            for r in data:
                soup = BeautifulSoup(r.get(), features="html.parser")
                description = soup.find('span',
                                        attrs={
                                            'class': 'GenresAndPlot__TextContainerBreakpointXL-cum89p-2 gCtawA'}).text

                _result.append(description)
    else:
        for i in movie_url_list:
            soup = helper_get_soup(i[1])
            description = soup.find('span',
                                    attrs={'class': 'GenresAndPlot__TextContainerBreakpointXL-cum89p-2 gCtawA'}).text

            _result.append(description)

    helper_file(data=_result, file_name=actor_name, directory=directory, to_do='write')

    return _result


def get_movie_distance_set(actor_start_url
                           , actor_end_urls
                           , num_of_actors_limit=None
                           , num_of_movies_limit=None
                           , clean_result_file=False
                           , parallel=8
                           , num_iteration=4
                           , print_logs=True):
    current_level = 1

    helper_file(file_name='actors', to_do='erase')
    helper_file(file_name='movies', to_do='erase')
    helper_file(file_name='actors_from_scan', to_do='erase')

    if clean_result_file:
        helper_file(file_name='results', to_do='erase')

    movie_urls = helper_url_collect(actor_start_url
                                    , func='actors'
                                    , file=True
                                    , to_do='append'
                                    , num_of_urls=num_of_movies_limit
                                    , print_logs=print_logs)

    print(f'************** LEVEL {current_level} **************\n')

    t1 = time.perf_counter()

    data = helper_url_collect_parallel(urls=movie_urls, func='movies', file=True, to_do='append',
                                       num_of_urls=num_of_actors_limit, parallel=parallel, print_logs=print_logs)

    actor_end_urls = set(actor_end_urls)

    data = set(data)

    helper_file(data, 'actors_from_scan', to_do='append', print_logs=print_logs)

    _intersection = actor_end_urls.intersection(data)

    print(f'movie_urls contains {len(movie_urls)} URLs')
    print(f'actor_end_urls contains {len(actor_end_urls)} URLs')
    print(f'data contains {len(data)} URLs')
    print(f'_intersection contains {len(_intersection)} URLs')

    if _intersection:
        print(f'{_intersection} was found on level {current_level}')
        res = [actor_start_url + ';' + actor_end_url + ';' + str(current_level) for actor_end_url in _intersection]
        helper_file(res, 'results', to_do='append')
        actor_end_urls -= _intersection

    if not actor_end_urls:
        return current_level

    t2 = time.perf_counter()

    print(f'Finished in {t2 - t1} seconds')

    actor_urls = helper_file(file_name='actors_from_scan', to_do='read', print_logs=print_logs)
    actor_urls = set(actor_urls)

    actor_urls_scanned = set(helper_file(file_name='actors', to_do='read', print_logs=print_logs))
    _intersection = actor_urls.intersection(actor_urls_scanned)
    actor_urls -= _intersection

    current_level += 1

    helper_calculate_level_set(urls=actor_urls
                               , actor_start_url=actor_start_url
                               , actor_end_urls=actor_end_urls
                               , num_of_actors_limit=num_of_actors_limit
                               , num_of_movies_limit=num_of_actors_limit
                               , parallel=parallel
                               , current_level=current_level
                               , num_iteration=num_iteration
                               , print_logs=print_logs)
