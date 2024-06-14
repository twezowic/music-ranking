import pandas as pd
import json
from sklearn.preprocessing import MinMaxScaler


def jsonl2df(path):
    with open(path) as fh:
        lines = fh.read().splitlines()

    df_inter = pd.DataFrame(lines)
    df_inter.columns = ['json_element']

    df_inter['json_element'].apply(json.loads)

    return pd.json_normalize(df_inter['json_element'].apply(json.loads))


def prepare_grouping(tracks="datav2/artists.jsonl", artists="datav2/tracks.jsonl", ranking_result=None, with_onehotencoding=False):
    # Load data
    artists = jsonl2df("datav2/artists.jsonl")
    tracks = jsonl2df("datav2/tracks.jsonl")

    # Get popular tracks within last week
    if ranking_result:
        tracks = tracks[tracks['id'].isin(ranking_result['track_id'])]

    # Join tracks with artists
    begin_prepared_data = pd.merge(tracks, artists, left_on="id_artist", right_on="id")

    # Remove rows with empty values
    begin_prepared_data = begin_prepared_data.dropna()
    begin_prepared_data = begin_prepared_data.drop(columns=["name_x", "popularity", "id_y", "name_y"])

    prepared_data = begin_prepared_data

    # Convert dates to number of days from earliest track
    prepared_data["release_date"] = pd.to_datetime(prepared_data["release_date"], format = "mixed")
    min_date = prepared_data['release_date'].min()
    prepared_data['release_date'] = (prepared_data['release_date'] - min_date).dt.days

    # Scale the attributes to [0, 1]
    without = ['id_x', 'duration_ms', 'id_artist', 'genres']

    # Droping explicit:
    without.append('explicit')

    scaled_columns = list(set(prepared_data.columns).difference(without))
    prepared_data[scaled_columns] = MinMaxScaler().fit_transform(prepared_data[scaled_columns])

    if with_onehotencoding:
        # One-hot encoding of genres
        genres_en = pd.get_dummies(prepared_data['genres'].apply(pd.Series).stack()).groupby(level=0).sum()
        prepared_data = pd.concat([prepared_data, genres_en], axis=1)

        # One-hot encoding of artists
        # artists_en = pd.get_dummies(prepared_data['id_artist']).groupby(level=0).sum()
        # prepared_data = pd.concat([prepared_data, artists_en], axis=1)

    prepared_data = prepared_data.drop(columns=['id_artist', 'genres'])

    return prepared_data
