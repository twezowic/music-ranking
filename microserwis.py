from crypt import methods
from flask import Flask, request, jsonify
from Ranking import Ranking
import pandas as pd
import numpy as np
from prepare_data import jsonl2df, prepare_grouping
from grouping.advanced_model import advanced_model
from ab_experiment import AB_experiment


class MicroSerwis:
    @property
    def app(self):
        return self._app

    @app.setter
    def app(self, app: Flask):
        self._app = app

    def __init__(self):
        self.app = Flask(__name__)
        self.setup_routes()
        self.model = Ranking(from_csv="sessions_popularity.csv", weeks=3, limit=0.25)
        self.model.group_by_weeks()
        self.tracks = jsonl2df("datav2/tracks.jsonl")
        self.artists = jsonl2df("datav2/artists.jsonl")

    def setup_routes(self):
        self.app.add_url_rule(rule="/get_playlists", endpoint="get_playlists", methods=["GET"], view_func=self.get_playlists)
        self.app.add_url_rule(rule="/get_ABExperiment", endpoint="get_ABExperiment", methods=["GET"], view_func=self.get_ABExperiment)
        self.app.add_url_rule(rule="/new_sessions", endpoint="new_sessions", methods=["POST"], view_func=self.post_new_session)
        self.app.add_url_rule(rule="/new_tracks", endpoint="new_tracks", methods=["POST"], view_func=self.post_new_tracks)
        self.app.add_url_rule(rule="/new_artists", endpoint="new_artists", methods=["POST"], view_func=self.post_new_artists)

    def get_playlists(self):
        year = request.args.get('year', default=None, type=np.int64)
        week = request.args.get('week', default=None, type=np.int64)
        tracks_id: pd.DataFrame = self.model.get_frame((year, week))

        prepared_data = prepare_grouping(tracks=self.tracks, artists=self.artists,
                                         ranking_result=tracks_id,
                                         with_onehotencoding=True)
        return advanced_model(prepared_data).set_index('id_x')['group'].to_dict()

    def get_ABExperiment(self):
        year = request.args.get('year', default=None, type=np.int64)
        week = request.args.get('week', default=None, type=np.int64)

        tracks_id_base: pd.DataFrame = self.model.get_frame((year, week), random=True)
        tracks_id_adv: pd.DataFrame = self.model.get_frame((year, week))

        return AB_experiment(tracks_id_base, tracks_id_adv,
                             self.tracks, self.artists)

    def post_new_session(self):
        new_data_json = request.json
        self.model.add_new(new_data_json)
        tracks_id: pd.DataFrame = self.model.get_tracks_for_last_week()

        prepared_data = prepare_grouping(tracks=self.tracks, artists=self.artists,
                                         ranking_result=tracks_id["track_id"],
                                         with_onehotencoding=True)
        return advanced_model(prepared_data).set_index('id_x')['group'].to_dict()

    def post_new_tracks(self):
        new_data_json = request.json
        new_data = pd.DataFrame(new_data_json)
        self.tracks = pd.concat([self.tracks, new_data], ignore_index=True)

    def post_new_artists(self):
        new_data_json = request.json
        new_data = pd.DataFrame(new_data_json)
        self.artists = pd.concat([self.artists, new_data], ignore_index=True)

    def run(self, *args, **kwargs):
        self.app.run(*args, **kwargs)


if __name__ == '__main__':
    app = MicroSerwis()
    app.run(host="0.0.0.0", port=8000)
