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

    # Create the requests session that will be used throughout the run and set the user-agent.
    # The user-agent requirements are specified at (https://e621.net/help/show/api#basics).
    with remote.requests_retry_session() as session:
        session.headers['User-Agent'] = constants.USER_AGENT
        
        # Check if a new version is released on github. If so, notify the user.
        if StrictVersion(constants.VERSION) < StrictVersion(remote.get_github_release(session)):
            print('A NEW VERSION OF e621dl IS AVAILABLE ON GITHUB: (https://github.com/Wulfre/e621dl/releases/latest).')

        print(f"[i] Running e621dl version {constants.VERSION}.")

        print('')

        print("[i] Checking for partial downloads...")

        remote.finish_partial_downloads(session)

        print('')

        print("[i] Parsing config...")
        config = local.get_config()

        # Initialize the lists that will be used to filter posts.
        blacklist = []
        searches = []

        # Initialize user configured options in case any are missing.
        include_md5 = False # The md5 checksum is not appended to file names.
        default_date = local.get_date(1) # Get posts from one day before execution.
        default_score = -0x7F_FF_FF_FF # Allow posts of any score to be downloaded.
        default_favs = 0
        default_ratings = ['s'] # Allow only safe posts to be downloaded.

        # Iterate through all sections (lines enclosed in brackets: []).
        for section in config.sections():

            # Get values from the "Other" section. Currently only used for file name appending.
            if section.lower() == 'other':
                for option, value in config.items(section):
                    if option.lower() == 'include_md5':
                        if value.lower() == 'true':
                            include_md5 = True

            # Get values from the "Defaults" section. This overwrites the initialized default_* variables.
            elif section.lower() == 'defaults':
                for option, value in config.items(section):
                    if option.lower() in {'days_to_check', 'days'}:
                        default_date = local.get_date(int(value))
                    elif option.lower() in {'min_score', 'score'}:
                        default_score = int(value)
                    elif option.lower() in {'min_favs', 'favs'}:
                        default_favs = int(value)
                    elif option.lower() in {'ratings', 'rating'}:
                        default_ratings = value.replace(',', ' ').lower().strip().split()

            # Get values from the "Blacklist" section. Tags are aliased to their acknowledged names.
            elif section.lower() == 'blacklist':
                blacklist = [remote.get_tag_alias(tag.lower(), session) for tag in config.get(section, 'tags').replace(',', ' ').lower().strip().split()]

            # If the section name is not one of the above, it is assumed to be the values for a search.
            else:

                # Initialize the list of tags that will be searched.
                section_tags = []

                # Default options are set in case the user did not declare any for the specific section.
                section_date = default_date
                section_score = default_score
                section_favs = default_favs
                section_ratings = default_ratings

                # Go through each option within the section to find search related values.
                for option, value in config.items(section):

                    # Get the tags that will be searched for. Tags are aliased to their acknowledged names.
                    if option.lower() in {'tags', 'tag'}:
                        section_tags = [remote.get_tag_alias(tag.lower(), session) for tag in value.replace(',', ' ').lower().strip().split()]

                    # Overwrite default options if the user has a specific value for the section
                    elif option.lower() in {'days_to_check', 'days'}:
                        section_date = local.get_date(int(value))
                    elif option.lower() in {'min_score', 'score'}:
                        section_score = int(value)
                    elif option.lower() in {'min_favs', 'favs'}:
                        section_favs = int(value)
                    elif option.lower() in {'ratings', 'rating'}:
                        section_ratings = value.replace(',', ' ').lower().strip().split()

                # Append the final values that will be used for the specific section to the list of searches.
                # Note section_tags is a list within a list.
                searches.append([section, section_tags, section_ratings, section_score, section_favs, section_date])

        for search in searches:
            print('')

            # Re-assign each element of the search list to an easier to remember name. There is probably a better way.
            directory = search[0]
            tags = search[1]
            ratings = search[2]
            min_score = search[3]
            min_favs = search[4]
            earliest_date = search[5]

            # Creates the string to be sent to the API.
            # Currently only 5 items can be sent directly so the rest are discarded to be filtered out later.
            if len(tags) > 5:
                search_string = ' '.join(tags[:5])
            else:
                search_string = ' '.join(tags)

            # Initializes last_id (the last post found in a search) to an enormous number so that the newest post will be found.
            # This number is hard-coded because on 64-bit archs, sys.maxsize() will return a number too big for e621 to use.
            last_id = 0x7F_FF_FF_FF

            # Sets up a loop that will continue indefinitely until the last post of a search has been found.
            while True:
                print("[i] Getting posts...")
                results = remote.get_posts(search_string, earliest_date, last_id, session)

                # Gets the id of the last post found in the search so that the search can continue.
                # If the number of results is less than the max, the next searches will always return 0 results.
                # Because of this, the last id is set to 0 which is the base case for exiting the while loop.
                if len(results) < constants.MAX_RESULTS:
                    last_id = 0
                else:
                    last_id = results[-1]['id']

                for post in results:
                    if include_md5:
                        path = local.make_path(directory, f"{post['id']}.{post['md5']}", post['file_ext'])
                    else:
                        path = local.make_path(directory, post['id'], post['file_ext'])

                    if os.path.isfile(path):
                        print(f"[✗] Post {post['id']} was already downloaded.")
                    elif post['rating'] not in ratings:
                        print(f"[✗] Post {post['id']} was skipped for missing a requested rating.")
                    # Using fnmatch allows for wildcards to be properly filtered.
                    elif [x for x in post['tags'].split() if any(fnmatch(x, y) for y in blacklist)]:
                        print(f"[✗] Post {post['id']} was skipped for having a blacklisted tag.")
                    elif not set(tags[4:]).issubset(post['tags'].split()):
                        print(f"[✗] Post {post['id']} was skipped for missing a requested tag.")
                    elif int(post['score']) < min_score:
                        print(f"[✗] Post {post['id']} was skipped for having a low score.")
                    elif int(post['fav_count']) < min_favs:
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
