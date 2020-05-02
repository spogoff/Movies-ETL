# import dependencies
import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from config import db_password
import os
import time


# make a variable for directory
file_dir = '../Movies-ETL-Resources'

#make a variable for database string
db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"


def transform_load(wikipedia_data,kaggel_metadata,rating_data):
    #create a list with movies that are listed with a directora dn an imdb link and they are not episodic
    #assumption = the input is a path to 2 csv files and a json file
    try:
        with open(wikipedia_data, mode='r', encoding="utf8") as file:
            wiki_movies_raw = json.load(file)
        kaggel_metadata = pd.read_csv(kaggel_metadata)
        ratings = pd.read_csv(rating_data)

    except:
        print("please enter your file path with this format f'{file_dir}/file_name")

    wiki_movies = [movie for movie in wiki_movies_raw 
               if 'Director' in movie or 'Directed by' in movie
               and 'imdb_link' in movie
               and 'No. of episodes' not in movie]



    
    def clean_movie(movie):
        #create a non-destructive copy
        movie = dict(movie)
        #create a dict for alt titles
        alt_titles ={}
        # combine alternate titles into one list
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune–Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles

        # merge column names
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')

        return movie

    #list comprehension applying clean_movies function on the entire list
    clean_movies = [clean_movie(movie) for movie in wiki_movies]

     #create a dataframe with clean wiki data
    wiki_movies_df = pd.DataFrame(clean_movies)


    #extract imdb id from url and remove duplicates
    wiki_movies_df["imdb_id"] = wiki_movies_df["imdb_link"].str.extract(r'(tt\d{7})')
    wiki_movies_df.drop_duplicates(subset="imdb_id", inplace=True)

    # update the dataframe so it has only columns we want to keep
    wiki_movies_df = wiki_movies_df[[column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]]


    #make a list from the Box office column and drop the null values and change datatype into string
    box_office = wiki_movies_df["Box office"].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)


    #create a regular expression for money formats
    money_form_one = r'\$\s*\d+\.?\d*\s*[bm]illi?on'
    money_form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+'

    #replace the ranges with a dollar sign
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    #extract all values in box office column that match either the first or second format
    box_office.str.extract(f'({money_form_one}|{money_form_two})')


    #make a function that drops extra strings and converts values into float data type
    def parse_dollars(s):

        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan
        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*millio?n',s,flags=re.IGNORECASE):
            # remove dollar sign and " million"
            s = re.sub(r'\$|\s|[a-zA-Z]','', s)
            # convert to float and multiply by a million
            value = float(s) * 10**6
            # return value
            return value
        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
            # remove dollar sign and " billion"
            s = re.sub(r'\$|\s|[a-zA-Z]','', s)
            # convert to float and multiply by a billion
            value = float(s) * 10**9
            # return value
            return value
        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
            # remove dollar sign and commas
            s = re.sub(r'\$|,','', s)
            # convert to float
            value = float(s)
            # return value
            return value
        # otherwise, return NaN
        else:
            return np.nan

    #apply the function to the box office column
    wiki_movies_df["box office"] = box_office.str.extract(f'({money_form_one}|{money_form_two})', flags = re.IGNORECASE)[0].apply(parse_dollars)

    #remove the old Box office column
    wiki_movies_df.drop('Box office', axis=1, inplace=True)


    #create a budget variable that excludes null values and convert to string
    budget = wiki_movies_df['Budget'].dropna().map(lambda x: ' '.join(x) if type(x) == list else x)


    #remove any values between a dollar sign and a hyphen 
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    #remove the citation reference 
    budget = budget.str.replace(r'\[\d+\]\s*', '')

    #apply the function to the budget column
    wiki_movies_df['budget'] = budget.str.extract(f'({money_form_one}|{money_form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)


    #remove the old budget column
    wiki_movies_df.drop('Budget', axis=1, inplace=True)

    # convert all non-null values of Release date to strings
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    #create regex for date forms
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'

    #extract the date matching the date forms above
    release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)

    # extract date time 
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)

    #remove the old Release date column
    wiki_movies_df.drop('Release date', axis=1, inplace=True)

    # turn running time to string
    running_time = wiki_movies_df["Running time"].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    #extract running time that follows either of the following formats
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')

    #change running time values to numeric and fill the Nan values with zero
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

    #change format to minutes
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)

    #remove the old budget column
    wiki_movies_df.drop("Running time", axis=1, inplace=True)

    #droping adult movies
    kaggel_metadata = kaggel_metadata[kaggel_metadata['adult'] == 'False'].drop('adult',axis='columns')


    #change datatypes in kaggle metadata
    #assumprtion = datatype is convertable to numeric, datetime and integer
    try:
        kaggel_metadata['budget'] = kaggel_metadata['budget'].astype(int)
        kaggel_metadata['id'] = pd.to_numeric(kaggel_metadata['id'], errors='raise')
        kaggel_metadata['popularity'] = pd.to_numeric(kaggel_metadata['popularity'], errors='raise')
        kaggel_metadata["release_date"] = pd.to_datetime(kaggel_metadata["release_date"])

    except TypeError as e:
        print(e)

    #change timestamp to date datatype
    ratings["timestamp"] = pd.to_datetime(ratings["timestamp"],unit="s")

    #merge kaggel and wiki dataframes 
    movies_df = pd.merge(wiki_movies_df, kaggel_metadata, on = "imdb_id", suffixes = ["-wiki", "-kaggel"])

    #drop the redundant column of title-wiki
    movies_df.drop("title-wiki", axis = 1, inplace = True)


    #drop language, production company, running time and release date from wiki dataframe in movies_df
    movies_df.drop(columns=["Language", "Production company(s)", "release_date-wiki"], inplace=True)

    #make a function to fill zero values from wiki columns in kaggle columns and then drop the wiki columns
    def fill_missing_kaggle_data(df,kaggle_column,wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis = 1)
        df.drop(columns=wiki_column, inplace=True)   


    #apply the function on the three columns for budget, revenue and run time
    fill_missing_kaggle_data(movies_df,'runtime',"running_time")
    fill_missing_kaggle_data(movies_df,'budget-kaggel','budget-wiki')
    fill_missing_kaggle_data(movies_df,'revenue','box office')


    #rename columns
    movies_df.rename({"id":"kaggle_id",
                  "title-kaggel":"title",
                  "url":"wikipedia_url",
                  "budget-kaggel":"budget",
                  "release_date_kaggle":"release_date",
                  "Country":"country",
                  "Distributor":"distributor",
                  "Producer(s)":"producers",
                  "Director":"director",
                  "Starring":"starring",
                  "Cinematography":"cinematography",
                  "Editor(s)":"editors",
                  "Writer(s)":"writers",
                  "Composer(s)":"composers",
                  "Based on": "based_on"
                }, axis="columns", inplace=True)


    #create pivot table that counts number of ratings for each rating value and puts them in rows by movie ID
    rating_counts = ratings.groupby(['movieId','rating'], as_index = False).count() \
                    .rename({'userId':'count'}, axis=1)\
                    .pivot(index='movieId', columns= 'rating', values='count')


    #change names in rating_counts df 
    rating_counts.columns = ["rating_" + str(col) for col in rating_counts.columns]


    #merge the rating_counts and movies dataframes
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on = 'kaggle_id', right_index = True, how='left')


    #fill ratings NaN values with 0
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    #movies_with_ratings_df


    #Create the database engine
    engine = create_engine(db_string)

    #save the movies_df DataFrame to a SQL table
    #assumption = the db_string variable has the right password
    try:
        movies_df.to_sql(name = 'movies', con = create_engine(db_string), if_exists = 'replace')

    except OperationalError:

        print("Did you put in the right password?")

    #reimport the ratings data in chunks and print out the number of rows imported as well as time elapsed

    rows_imported = 0
    start_time = time.time()
    for data in pd.read_csv(f'{file_dir}/ratings.csv', chunksize=1000000):
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        if rows_imported == 0:
             data.to_sql(name='ratings', con=engine, if_exists='replace')
        else:
             data.to_sql(name='ratings', con=engine, if_exists='append')
        rows_imported += len(data)

    print(f'Done. {time.time() - start_time} total seconds elapsed')

    print(movies_df.columns.to_list())
        
        
