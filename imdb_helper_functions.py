import os
import requests
from bs4 import BeautifulSoup
import re
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import multiprocessing



def helper_www_add(url):
    if url.count('www') == 0:
        return url.replace('https://', 'https://www.')
    else:
        return url


def helper_get_soup(url, n_sec=10, content=False):
    session = requests.Session()
    retry = Retry(connect=60, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    request = session.get(url, headers={'Accept-Language': 'en'})

    if request.status_code == 200:
        if content:
            return request.content
        return BeautifulSoup(request.content, features="html.parser")

    elif request.status_code == 503:
        print(f'Wait {n_sec} seconds')
        time.sleep(n_sec)
        return helper_get_soup(url, n_sec)

    return None


def helper_www_add_set(urls):
    _set = set()
    for url in urls:
        _set |= helper_www_add(url)
    return _set


def helper_get_actor_id(url):
    return re.findall(r'nm[\w+]*', url)[0]


def helper_check_brackets(findall_res):
    if re.findall(r'\((.*)\)', str(findall_res)):
        return set(map(str.strip, re.findall(r'\((.*)\)', str(findall_res))[0].split(',')))
    else:
        return set()


def helper_get_info_actor(soup):
    list_ = ['TV Series', 'Short', 'Video Game', 'Video short', 'Video', 'TV Movie'
        , 'TV Mini-Series', 'TV Series short', 'TV Special', 'voice', 'TV Mini Series', 'Music Video'
        , 'Music Video short']
    _res = [i for i in soup.find_all('div', ['filmo-row odd', 'filmo-row even']) if
            i.get('id') is not None and (i.get('id').count('actor') > 0 or i.get('id').count('actress') > 0)]
    _lst = [
        (i.a.text
         , 1 if i.find_all('a', {'class': 'in_production'}) else 0
         , 1 if helper_check_brackets(i).intersection(set(list_)) else 0
         , 'https://www.imdb.com' + i.a['href']
         ) for i in _res
    ]
    return [(name, url) for name, prod, not_movie, url in _lst if prod != 1 and not_movie != 1]


def helper_get_info_movie(soup):
    _lst = list(zip([i.find('a', href=True, text=True).text.strip()
                     for i in soup.find_all('table', {'class': 'cast_list'})[0].find_all('td')
                     if i.find('a', href=True, text=True) != None
                     and re.findall('name', str(i.find('a', href=True, text=True)))
                     ]

                    , [
                        'https://www.imdb.com' + i.a['href']
                        for i in soup.find_all('table', {'class': 'cast_list'})[0].find_all('td')
                        if i.find('a', href=True, text=True) != None
                        and re.findall('name', str(i.find('a', href=True, text=True)))

                    ]

                    )
                )

    return _lst


def helper_url_collect(url, func='movies', n_sec=10, file=False, to_do='write', num_of_urls=None, print_logs=True):

    soup = helper_get_soup(url, n_sec)

    url_list = []

    if soup is not None:

        if print_logs:
            print(url)

        if func == 'actors':
            _list = helper_get_info_actor(soup)
            url_list = [url[1] + 'fullcredits' for url in _list]


        elif func == 'movies':
            _list = helper_get_info_movie(soup)
            url_list = [url[1] for url in _list]

        if file:
            _insert = []
            _insert.append(url)
            helper_file(data=_insert, file_name=func, to_do=to_do, print_logs=print_logs)

        if num_of_urls is not None:
            url_list = url_list[:num_of_urls]

        return url_list

    return []


def helper_url_collect_parallel(urls
                                , func='movies'
                                , n_sec=10
                                , file=False
                                , to_do='write'
                                , num_of_urls=None
                                , parallel=8
                                , print_logs=True):
    _set = set()
    with multiprocessing.Pool(processes=parallel) as pool:
        data = [pool.apply_async(helper_url_collect, args=(url, func, n_sec, file, to_do, num_of_urls, print_logs))
                for url in urls]

        for r in data:
            _set |= set(r.get())

    return list(_set)


def helper_file(data='', file_name='Default', directory=os.getcwd(), to_do='write', print_logs=True):
    if to_do == 'write':
        with open(directory + '/' + file_name + '.txt', 'w') as f:
            for line in data:
                f.write(line)
                f.write('\n')

        if print_logs:
            print(f'File {file_name}.txt is written in {directory}')

    elif to_do == 'append':
        with open(directory + '/' + file_name + '.txt', 'a') as f:
            for line in data:
                f.write(line)
                f.write('\n')

        if print_logs:
            print(f'File {file_name}.txt appends lines!')

    elif to_do == 'erase':
        try:
            with open(directory + '/' + file_name + '.txt', 'r+') as f:
                f.truncate(0)
        except OSError as e:
            print(e)

        if print_logs:
            print(f'File {file_name}.txt was erased!')

    else:
        with open(directory + '/' + file_name + '.txt', 'r') as f:
            _l = f.read().splitlines()
        return _l


def helper_calculate_level_set(urls
                               , actor_start_url
                               , actor_end_urls
                               , num_of_actors_limit=None
                               , num_of_movies_limit=None
                               , parallel=None
                               , current_level=1
                               , num_iteration=4
                               , print_logs=True):

    print(f'************** LEVEL {current_level} **************\n')

    actor_end_urls = set(actor_end_urls)

    print('\n', f'actor_end_urls contains {len(actor_end_urls)} URLs')

    if current_level == num_iteration:
        print(f'{actor_end_urls} was found on level -1')
        res = [actor_start_url + ';' + actor_end_url + ';' + '-1' for actor_end_url in actor_end_urls]
        helper_file(res, 'results', to_do='append', print_logs=print_logs)
        return -1

    with multiprocessing.Pool(processes=parallel) as pool:
        movie_urls = set()
        data = [pool.apply_async(helper_url_collect, args=(url, 'actors', 10, True, 'append', None, print_logs)) for url in
                urls]
        for r in data:
            movie_urls_scanned = set(helper_file(file_name='movies', to_do='read', print_logs=print_logs))
            _res = set(r.get())
            _intersection = movie_urls_scanned.intersection(_res)
            _res -= _intersection
            _res = set(list(_res)[:num_of_movies_limit])
            movie_urls |= _res

    print(f'++++++++++++++++++ {len(movie_urls)} movies for scanning ++++++++++++++++++++')

    with multiprocessing.Pool(processes=parallel) as pool:

        data = [pool.apply_async(helper_url_collect,
                                 args=(url, 'movies', 10, True, 'append', num_of_actors_limit, print_logs))
                for url in movie_urls]

        for r in data:

            helper_file(r.get(), 'actors_from_scan', to_do='append', print_logs=print_logs)
            _intersection = actor_end_urls.intersection(set(r.get()))

            if _intersection:
                print(f'{_intersection} was found on level {current_level}')
                res = [actor_start_url + ';' + actor_end_url + ';' + str(current_level) for actor_end_url in
                       _intersection]
                helper_file(res, 'results', to_do='append')
                actor_end_urls = actor_end_urls.difference(_intersection)

            if not actor_end_urls:
                return current_level

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
