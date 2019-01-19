#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Internal Imports
import os
from distutils.version import StrictVersion
from fnmatch import fnmatch

# Personal Imports
from e621dl import constants
from e621dl import local
from e621dl import remote

# This block will only be read if e621dl.py is directly executed as a script. Not if it is imported.
if __name__ == '__main__':
    # Create the requests session that will be used throughout the run.
    with remote.requests_retry_session() as session:
        # Set the user-agent. Requirements are specified at https://e621.net/help/show/api#basics.
        session.headers['User-Agent'] = f"e621dl.py/{constants.VERSION} (by Wulfre)"
        
        # Check if a new version is released on github. If so, notify the user.
        if StrictVersion(constants.VERSION) < StrictVersion(remote.get_github_release(session)):
            print('A NEW VERSION OF e621dl IS AVAILABLE ON GITHUB AT https://github.com/Wulfre/e621dl/releases/latest.')

        print(f"[i] Running e621dl version {constants.VERSION}.")
        print('')
        print("[i] Checking for partial downloads...")

        remote.finish_partial_downloads(session)

        print('')
        print("[i] Parsing config...")

        config = local.get_config()

        # Initialize the lists that will be used to filter posts.
        searches = []

        # Initialize user configured options in case any are missing.
        include_md5 = config['toggles'].get('include_md5', False)
        default_days = config['default_search'].get('days', 1)
        default_score = config['default_search'].get('min_score', -0x7F_FF_FF_FF)
        default_favs = config['default_search'].get('min_favs', 0)
        default_ratings = config['default_search'].get('ratings', ['s'])

        if config['blacklist']:
            blacklist = [remote.get_tag_alias(tag.lower(), session) for tag in config['blacklist']]
        else:
            blacklist = []

        for key, value in config['searches'].items():
            # Get the tags that will be searched for. Tags are aliased to their acknowledged names.
            section_tags = [remote.get_tag_alias(tag.lower(), session) for tag in value['tags']]

            # Replace options that are specified by the user.
            section_date = local.get_date(value.get('days', default_days))
            section_score = value.get('min_score', default_score)
            section_favs = value.get('min_favs', default_favs)
            section_ratings = value.get('ratings', default_ratings)

            # Append the final values that will be used for the specific section to the list of searches.
            # Note section_tags is a list within a list.
            searches.append({'directory': key, 'tags': section_tags, 'ratings': section_ratings, 'min_score': section_score, 'min_favs': section_favs, 'earliest_date': section_date})

        for search in searches:
            print('')

            # Creates the string to be sent to the API.
            # Currently only 5 items can be sent directly so the rest are discarded to be filtered out later.
            if len(search['tags']) > 5:
                search_string = ' '.join(search['tags'][:5])
            else:
                search_string = ' '.join(search['tags'])

            # Initializes last_id (the last post found in a search) to an enormous number so that the newest post will be found.
            # This number is hard-coded because on 64-bit archs, sys.maxsize() will return a number too big for e621 to use.
            last_id = 0x7F_FF_FF_FF

            # Sets up a loop that will continue indefinitely until the last post of a search has been found.
            while True:
                print("[i] Getting posts...")
                results = remote.get_posts(search_string, search['earliest_date'], last_id, session)

                # Gets the id of the last post found in the search so that the search can continue.
                # If the number of results is less than the max, the next searches will always return 0 results.
                # Because of this, the last id is set to 0 which is the base case for exiting the while loop.
                if len(results) < constants.MAX_RESULTS:
                    last_id = 0
                else:
                    last_id = results[-1]['id']

                for post in results:
                    if include_md5:
                        path = local.make_path(search['directory'], f"{post['id']}.{post['md5']}", post['file_ext'])
                    else:
                        path = local.make_path(search['directory'], post['id'], post['file_ext'])

                    if os.path.isfile(path):
                        print(f"[✗] Post {post['id']} was already downloaded.")
                    elif post['rating'] not in search['ratings']:
                        print(f"[✗] Post {post['id']} was skipped for missing a requested rating.")
                    # Using fnmatch allows for wildcards to be properly filtered.
                    elif [x for x in post['tags'].split() if any(fnmatch(x, y) for y in blacklist)]:
                        print(f"[✗] Post {post['id']} was skipped for having a blacklisted tag.")
                    elif not set(search['tags'][4:]).issubset(post['tags'].split()):
                        print(f"[✗] Post {post['id']} was skipped for missing a requested tag.")
                    elif int(post['score']) < search['min_score']:
                        print(f"[✗] Post {post['id']} was skipped for having a low score.")
                    elif int(post['fav_count']) < search['min_favs']:
                        print(f"[✗] Post {post['id']} was skipped for having a low favorite count.")
                    else:
                        print(f"[✓] Post {post['id']} is being downloaded.")
                        remote.download_post(post['file_url'], path, session)

                # Break while loop. End program.
                if last_id == 0:
                    break

    # End program.
    print('')
    input("[✓] All searches complete. Press ENTER to exit...")
    raise SystemExit
