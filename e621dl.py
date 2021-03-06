#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Internal Imports
import os
import re
from distutils.version import StrictVersion
from shutil import copy
from threading import Thread
from time import sleep
from concurrent.futures import ThreadPoolExecutor
from traceback import print_exc

# Personal Imports
from e621dl_lib import constants
from e621dl_lib import local
from e621dl_lib import remote

# External Imports

from requests.exceptions import HTTPError

download_queue = local.DownloadQueue()
config_queue = local.ConfigQueue()

storage = local.PostsStorage()
download_set = local.ActiveDownloadsSet()

def is_prefilter(section_name):
    return 'prefilter' == section_name or ( section_name[0]=='<' and section_name[-1] == '>' )

def default_condition(x):
    return True

def check_has_actual_search(whitelist, blacklist, anylist, cond_func, **dummy):
    return whitelist or blacklist or anylist or cond_func != default_condition
    

def process_result(post, whitelist, blacklist, anylist, cond_func, ratings, min_score, min_favs, days_ago, has_actual_search, **dummy):
    tags = post.tags

    if not has_actual_search:
        return []
    if whitelist and not all( any(reg.fullmatch(tag) for tag in tags) for reg in whitelist ):
        return []
    elif blacklist and any( any(reg.fullmatch(tag) for tag in tags) for reg in blacklist ):
        return []
    elif anylist and not any(any(reg.fullmatch(tag) for tag in tags) for reg in anylist):
        return []
    elif not cond_func(set(tags)):
        return []
    elif post.rating not in ratings:
        return []
    elif int(post.score) < min_score:
        return []
    elif int(post.fav_count) < min_favs:
        return []
    elif post.days_ago >= days_ago:
        return []   
    else:
        return [post]
        

def process_results(results, **dummy):
    filtered_results=[]

    for post in results:
        filtered_results += process_result(post, **dummy)
        
    return filtered_results

#TODO: describe how this all works. God this is not intuitive
def get_directories(post, root_dirs, search, searches_dict):
    subdirectories = search['subdirectories']
    
    # below lies recursion
    # Essentially we travel down the tree
    # until we get to branch with no subbranches
    results = []
    
    search_result = process_result(post, **search)
    
    # We travel below only if current folder matches
    # our criteria or there is nothing to look for
    if search_result or not search['has_actual_search']:
        for directory in subdirectories:
            #preventing recursions in cases like cat/dog/cat/dog/...
            if directory in root_dirs:
                continue

            results += get_directories(post, root_dirs + [directory], searches_dict[directory], searches_dict)
    # And for each branch on the same level,
    # We check if we should place files there.
    # If we find matching folder on a deeper level
    # (that is, results are not empty),
    # we won't place file on a current level.
    # If not, we check if current folder
    # matches and if it is, we place our file there
    if not results and search_result:
        return ['/'.join(root_dirs)]
    else:
        return results
    

def get_files(post, filename, directories, files, session, cachefunc, duplicate_func, download_post, search, api_key, login):
    with download_set.context_id(post.id):

        
        for directory in directories:
            file_id=post.id
            path = local.make_path(directory, filename)

            if os.path.isfile(path):
                local.printer.increment_old()
            elif file_id in files:
                duplicate_func(files[file_id], path)
                local.printer.increment_copied()
            else:
                if download_post(post.file_url, path, session, cachefunc, duplicate_func, api_key, login):
                    files[file_id]=path
                    local.printer.increment_downloaded()
                else:
                    local.printer.increment_not_found()
                    return search, False
        
        return search, True
                
#@profile
def prefilter_build_index(kwargses, use_db, searches):
    
    if use_db:
        storage.connect()
    
    blocked_ids = local.get_blocked_posts()
    
    try:
        if download_queue.completed:
            return
        
        last_id = download_queue.last_id
        
        for kwargs in kwargses:

            directory = kwargs['directory']
            local.printer.change_section(directory)
            gen = kwargs['gen_funcs']
            append_func=kwargs['append_func']
            max_days_ago=kwargs['days_ago']
            
            for results in gen(last_id, **kwargs):
                local.printer.increment_posts(len(results))
                append_func(results)
                filtered_results=[post for post in results if post.id not in blocked_ids]
                filtered_results=process_results(filtered_results, **kwargs)
                local.printer.increment_filtered(len(set(results) - set(filtered_results)))
                
                download_queue.append( (directory, filtered_results) )
                post=results[-1]
                download_queue.last_id=post.id
                if post.days_ago >= max_days_ago:
                    break
                
                if not any(s for s in searches if s['posts_countdown'] > 0):
                    break
            last_id = None
            download_queue.completed_gen(directory)
        download_queue.completed = True
    except HTTPError as e:
        local.printer.show(False)
        local.printer.stop()
        local.printer.join()
        local.printer.reset_screen()
        print("Exception in api iterator:")
        print_exc()
        print("Http Status: ", e.response.status_code)
        print("Text: ", e.response.text)
    except:
        local.printer.show(False)
        local.printer.stop()
        local.printer.join()
        local.printer.reset_screen()
        print("Exception in api iterator:")
        print_exc()
    finally:
        download_queue.aborted = True
        if use_db:
            storage.close()
          
          
def main():
    #local.printer.show(False)
    local.printer.start()
    local.save_on_exit_events(download_queue.save)
    current_configs = local.get_configs()
    config_queue.change_if_not_same(current_configs)
    config_queue.reset_if_complete()
    
    local.printer.change_status("Building downloaded files dict")
    files = local.get_files_dict(config_queue.reset_filedb)
    
    
    pathes_storage=local.PathesStorage()
    config_queue.reset_filedb = False
    config_queue.save()
    prune_downloads = False
    prune_cache = False
    
    with remote.requests_retry_session() as session:
        for config in config_queue.get_remaining():
            config_name = '/'.join(config.replace('\\','/').split('/')[1:])
            local.printer.change_config(config_name)
            config_prune_downloads, config_prune_cache = \
                process_config(config, session, files, pathes_storage)
            
            prune_downloads = prune_downloads or config_prune_downloads
            prune_cache = prune_cache or config_prune_cache
            
            config_queue.add(config)
            config_queue.save()
    

    if prune_downloads:
        local.printer.change_status("Pruning downloads")
        pathes_storage.remove_old()
    
    if prune_cache:
        local.printer.change_status("Pruning cache")
        local.prune_cache()
    
    local.printer.change_status("Removing empty folders")
    local.remove_empty_folders()

    local.printer.change_status("All complete")
    local.printer.stop()
    local.printer.join()
    local.printer.step()
    
    

#@profile
def process_config(filename, session, files, pathes_storage):
    # Create the requests session that will be used throughout the run.
    
    # local.printer.show(False)
    
    
    
    
    # Set the user-agent. Requirements are specified at https://e621.net/help/show/api#basics.
    session.headers['User-Agent'] = f"e621dl (lurkbbs) -- Version {constants.VERSION}"
    
    local.printer.change_status("Parsing config")

    config, hash = local.get_config(filename)
    download_queue.check_config_hash(hash)
    download_queue.aborted = False

    # Initialize the lists that will be used to filter posts.
    blacklist = []
    searches = []
    searches_dict = {}

    # Initialize user configured options in case any are missing.
    include_md5 = False # The md5 checksum is not appended to file names.
    default_days_ago = 1
    default_date = local.get_date(default_days_ago) # Get posts from one day before execution.
    default_score = -0x7F_FF_FF_FF # Allow posts of any score to be downloaded.
    default_favs = 0
    default_ratings = ['s'] # Allow only safe posts to be downloaded.
    default_posts_limit = float('inf')
    default_format = ''
    default_subdirectories = set()
    
    duplicate_func = copy
    cachefunc = None
    prefilter = []
    max_days_ago = default_days_ago
    cond_func = lambda x: True
    default_gen_func = remote.get_posts
    default_append_func = lambda x: None
    
    get_tag_alias = remote.get_tag_alias
    download_post = remote.download_post
    
    use_db = False
    allow_append = False
    full_offline = False
    prune_downloads = False
    prune_cache = False
    api_key = None
    login = None
    # Iterate through all sections (lines enclosed in brackets: []).
    for section in config.sections():

        make_cache_flag=False
        # Get values from the "Settings" section. Currently only used for file name appending.
        if section.lower() == 'settings':
            for option, value in config.items(section):
                if option.lower() == 'include_md5':
                    if value.lower() == 'true':
                        include_md5 =  True
                elif option.lower() == 'make_hardlinks':
                    if value.lower() == 'true':
                        duplicate_func = os.link
                elif option.lower() == 'make_cache':
                    if value.lower() == 'true':
                        local.make_cache_folder()
                        make_cache_flag=True
                elif option.lower() in {'maintain_db','db','use_db','database', 'maintain_database' }:
                    if value.lower() == 'true':
                        default_append_func = storage.append
                        use_db = True
                        allow_append = True
                elif option.lower() in {'prune_downloads'}:
                    if value.lower() == 'true':
                        prune_downloads = True
                elif option.lower() in {'prune_cache'}:
                    if value.lower() == 'true':
                        prune_cache = True                
                elif option.lower() in {'password', 'api_key', 'key'}:
                        api_key = value.strip().lower()
                elif option.lower() in {'login', 'username', 'name'}:
                        login = value.strip().lower()
                
        if section.lower() == 'settings':
            for option, value in config.items(section):
                if option.lower() in {'full_offline', 'offline'}:
                    if value.lower() == 'true':
                        default_gen_func=storage.gen
                        default_append_func = lambda x: None
                        
                        get_tag_alias = lambda _tag, _api_key, _login, _session: _tag
                        download_post = lambda _file_url, _path, _session, _cachefunc, _duplicate_func, _api_key, _login : False
                        
                        use_db = True
                        allow_append = False
                        full_offline = True

            if make_cache_flag:
                cachefunc = duplicate_func

        # Get values from the "Defaults" section. This overwrites the initialized default_* variables.
        elif section.lower() == 'defaults':
            for option, value in config.items(section):
                if option.lower() in {'days_to_check', 'days'}:
                    default_days_ago = int(value)
                    default_date = local.get_date(default_days_ago)
                    max_days_ago = max(max_days_ago, default_days_ago)
                elif option.lower() in {'min_score', 'score'}:
                    default_score = int(value)
                elif option.lower() in {'min_favs', 'favs'}:
                    default_favs = int(value)
                elif option.lower() in {'ratings', 'rating'}:
                    default_ratings = value.replace(',', ' ').lower().strip().split()
                elif option.lower() in {'limit', 'max_downloads', 'posts_limit', 'files_limit'}:
                    if value.lower() != 'inf':
                        default_posts_limit = int(value)
                    else:
                        default_posts_limit = float('inf')
                elif option.lower() in {'format', 'default_format'}:
                    default_format = value.strip()
                elif option.lower() in {'posts_from', 'posts_func', 'posts_source', 'post_from','post_func', 'post_source'}:
                    if value.lower() in {'db','database','local'}:
                        default_gen_func=storage.gen
                        default_append_func = lambda x: None
                        use_db = True
                elif option.lower() in {'subfolder', 'subfolders', 'subdir', 'subdirs', 'subdirectory', 'subdirectories'}:
                    default_subdirectories.update( value.replace(',', ' ').lower().strip().split() )
                
        # Get values from the "Blacklist" section. Tags are aliased to their acknowledged names.
        elif section.lower() == 'blacklist':
            for option, value in config.items(section):
                if option.lower() in {'tags', 'tag'}:
                    blacklist = [get_tag_alias(tag.lower(), api_key, login, session) for tag in value.replace(',', ' ').lower().strip().split()]

    # Making use of include_md5
    if include_md5 and len(default_format) == 0:
        default_format = '{id}.{md5}'

    # making a set of all sections
    all_sections_list = set()
    for section in config.sections():
        section_id = section.lower().strip()
        if section_id in {'settings','defaults','blacklist'}:
            continue
        
        if is_prefilter(section_id):
            continue
        
        if section_id[0] == "*":
            section_directory = section_id[1:]
        else:
            section_directory = section_id
        
        all_sections_list.add(section_directory)
        
    # checking if all subfolders in folders are correct
    for section in config.sections():
        section_id = section.lower().strip()
        if section_id in {'settings','defaults','blacklist'}:
            continue
        
        if is_prefilter(section_id):
            continue
            
        for option, value in config.items(section):
            op_low = option.lower()
            if op_low in {'subfolder', 'subfolders', 'subdir', 'subdirs', 'subdirectory', 'subdirectories'}:
                for subfolder in value.replace(',', ' ').lower().strip().split():
                    if subfolder not in all_sections_list:
                        local.printer.show(False)
                        local.printer.stop()
                        local.printer.join()
                        local.printer.reset_screen()
                        print(f'[!] Error in section "{section}":')
                        print(f'subfolder "{subfolder}" does not exists')
                        download_queue.save()
                        os._exit(0)
    # If the section name is not one of the above, it is assumed to be the values for a search.
    # two for cycles in case of e.g 'blacklist' is in the end of a config file 
    for section in config.sections():
        section_id = section.lower().strip()
        if section_id not in {'settings','defaults','blacklist'}:

            # Initialize the list of tags that will be searched.
            section_tags = []

            # Default options are set in case the user did not declare any for the specific section.
            section_date = default_date
            section_score = default_score
            section_favs = default_favs
            section_ratings = default_ratings
            section_cond_func = default_condition
            section_blacklist = []
            section_whitelist = []
            section_anylist = []
            section_blacklisted = []
            section_days_ago = default_days_ago
            section_gen_func = default_gen_func
            section_append_func = default_append_func
            section_post_limit = default_posts_limit
            section_format = default_format
            section_subdirectories = set() #default_subdirectories.copy()
            use_default_subfolders = True
            # Go through each option within the section to find search related values.
            for option, value in config.items(section):

                # Get the tags that will be searched for. Tags are aliased to their acknowledged names.
                if option.lower() in {'tags', 'tag'}:
                    section_tags = [get_tag_alias(tag.lower(), api_key, login, session) for tag in value.replace(',', ' ').lower().strip().split()]
                    section_blacklist += [tag[1:] for tag in section_tags if tag[0]=='-']
                    section_anylist   += [tag[1:] for tag in section_tags if tag[0]=='~']
                    section_whitelist += [tag for tag in section_tags if tag[0] not in ('-','~')]
                    
                elif option.lower() in {'subfolder', 'subfolders', 'subdir', 'subdirs', 'subdirectory', 'subdirectories'}:
                    section_subdirectories.update( value.replace(',', ' ').lower().strip().split() )
                # Overwrite default options if the user has a specific value for the section
                elif option.lower() in {'days_to_check', 'days'}:
                    section_days_ago=int(value)
                    section_date = local.get_date(section_days_ago)
                    max_days_ago = max(max_days_ago, section_days_ago)
                elif option.lower() in {'blacklist', 'blacklist_tags', 'blacklisted'}:
                    section_blacklisted = [get_tag_alias(tag.lower(), api_key, login, session) for tag in value.replace(',', ' ').lower().strip().split()]
                elif option.lower() in {'min_score', 'score'}:
                    section_score = int(value)
                elif option.lower() in {'min_favs', 'favs'}:
                    section_favs = int(value)
                elif option.lower() in {'blacklist_default_subfolders', 'no_default_subfolders'}:
                    use_default_subfolders = not (value.lower() == "true")
                elif option.lower() in {'ratings', 'rating'}:
                    section_ratings = value.replace(',', ' ').lower().strip().split()
                elif option.lower() in {'limit', 'max_downloads', 'posts_limit', 'files_limit'}:
                    if value.lower() != 'inf':
                        section_post_limit = int(value)
                    else:
                        section_post_limit = float('inf')
                elif option.lower() in {'format', 'default_format'}:
                    section_format = value.strip()
                elif option.lower() in {'condition', 'conditions'}:
                    if value.lower().strip():
                        source_template, tags = local.tags_and_source_template(value.lower().strip())
                        tags = [get_tag_alias(tag.lower(), api_key, login, session) for tag in tags]
                        section_cond_func = local.make_check_funk(source_template, tags)
                elif option.lower() in {'posts_from', 'posts_func', 'posts_source', 'post_from', 'post_func', 'post_source'}:
                    if value.lower() in {'db','database','local'}:
                        section_gen_func=storage.gen
                        section_append_func = lambda x: None
                        use_db = True
                    elif not full_offline:
                        section_gen_func=remote.get_posts
                        if allow_append:
                            section_append_func = storage.append
            
            section_tags += ['-'+tag for tag in blacklist+section_blacklisted]
            #section_search_tags = [tag for tag in section_tags if '*' not in tag][:38]
            section_search_tags = section_tags[:constants.MAX_USER_SEARCH_TAGS]
            section_blacklist=[re.compile(re.escape(mask).replace('\\*','.*')) for mask in section_blacklist+section_blacklisted]
            section_whitelist=[re.compile(re.escape(mask).replace('\\*','.*')) for mask in section_whitelist]
            section_anylist = [re.compile(re.escape(mask).replace('\\*','.*')) for mask in section_anylist]
            
            section_has_actual_search = \
                check_has_actual_search(section_whitelist, section_blacklist, section_anylist, section_cond_func)
            if section_has_actual_search and use_default_subfolders:
                section_subdirectories.update(default_subdirectories)
            # Append the final values that will be used for the specific section to the list of searches.
            # Note section_tags is a list within a list.
            
            section_blacklist +=[re.compile(re.escape(mask).replace('\\*','.*')) for mask in blacklist]
            
            if section_id[0] == "*":
                section_directory = section_id[1:]
            else:
                section_directory = section_id
                
            section_dict = { 'directory': section_directory,
                             'search_tags': section_search_tags,
                             'ratings': section_ratings,
                             'min_score': section_score,
                             'min_favs': section_favs, 
                             'earliest_date': section_date, 
                             'days_ago': section_days_ago, 
                             'blacklist': section_blacklist, 
                             'whitelist': section_whitelist, 
                             'anylist': section_anylist,
                             'cond_func': section_cond_func,
                             'gen_funcs': section_gen_func,
                             'append_func': section_append_func,
                             'posts_countdown': section_post_limit,
                             'format':section_format,
                             'subdirectories': section_subdirectories,
                             'session'  : session,
                             'has_actual_search': section_has_actual_search,
                             'login': login,
                             'api_key': api_key,}
            
            if is_prefilter(section_id):
                prefilter.append(section_dict)
            else:
                searches_dict[section_directory] = section_dict
                if section_id[0] != "*":
                    searches.append(section_dict)

    local.printer.change_tag("all tags are valid")
    local.printer.change_status("Checking for partial downloads")

    if not full_offline:
        remote.finish_partial_downloads(session, cachefunc, duplicate_func, api_key, login)
    
    
    
    if prefilter:
        for pf in prefilter:
            pf['days_ago'] = max_days_ago
        kwargs = prefilter
    else:
        kwargs = [search for search in searches if not download_queue.in_gens(search['directory'])]

    local.printer.change_status("Downloading files")
    queue_thread=Thread(target=prefilter_build_index, args=(kwargs, use_db, searches))
    queue_thread.start()
    
    download_pool=ThreadPoolExecutor(max_workers=2)
    
    try:
        while True:
            try:
                chunk_directory, chunk = download_queue.first()
            except:
            
                if download_queue.aborted:
                    break
                else:
                    sleep(0.5)
                    continue
    
            results_pair = []
            for search in searches:
                directory = search['directory']
                if chunk_directory.lower() != directory.lower() and not is_prefilter(chunk_directory.lower()):
                    continue

                results_pair += list(zip([search]*len(chunk), chunk))
            
            while results_pair:
                futures = []
                remaining_from_countdown=[]
                
                pathes_storage.begin()
                for search, post in results_pair:
                    directory = search['directory']
                    format = search['format']
                    if search['posts_countdown'] <= 0:
                        remaining_from_countdown.append( (search, post) )
                        continue
                    
                    directories = get_directories(post, [directory], search, searches_dict)
                    if directories:
                        if format:
                            id_ext = f'{post.id}.{post.file_ext}'
                            custom_prefix = format.format(**post.generate())[:100]
                            filename = f'{custom_prefix}.{id_ext}'
                        else:
                            filename = f'{post.id}.{post.file_ext}'
                        
                        pathes_storage.add_pathes(directories, filename)
                        futures.append(download_pool.submit(get_files,
                            post, filename, directories, files,
                            session, cachefunc, duplicate_func, download_post, search, api_key, login))
                        
                        search['posts_countdown'] -= 1
                    else:
                        local.printer.increment_filtered(1)
                pathes_storage.commit()
                
                for future in futures:
                    if future.exception():
                        raise future.exception()
                    
                    #Recovering wrong countdown decrement
                    #Still not good and may lead to less post than
                    #max_posts. But better than it was
                    search, success = future.result()
                    if not success:
                        search['posts_countdown'] += 1
                
                results_pair = []
                for search, post in remaining_from_countdown:
                    if search['posts_countdown'] > 0:
                        results_pair.append(search, post)
                    
            download_queue.popleft()

    except: #Pull request a better way
        local.printer.show(False)
        local.printer.stop()
        local.printer.join()
        local.printer.reset_screen()
        print("Exception during download:")
        print_exc()
        download_queue.save()
        os._exit(0)
    
    queue_thread.join()
    
    if download_queue.completed:
        download_queue.reset()
    
    return prune_downloads, prune_cache
    
    
    
# This block will only be read if e621dl.py is directly executed as a script. Not if it is imported.
if __name__ == '__main__':
    main()
