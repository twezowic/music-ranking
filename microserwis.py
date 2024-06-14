from crypt import methods
from flask import Flask, request, jsonify
from Ranking import Ranking
import pandas as pd
import numpy as np


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

    def setup_routes(self):
        self.app.add_url_rule(rule="/get_playlists", endpoint="get_playlists", methods=["GET"], view_func=self.get_playlists)
        self.app.add_url_rule(rule="/new_sessions", endpoint="new_sessions", methods=["POST"], view_func=self.post_new)

    def get_playlists(self):
        year = request.args.get('year', default=None, type=np.int64)
        week = request.args.get('week', default=None, type=np.int64)
        tracks_id: pd.DataFrame = self.model.get_frame((year, week))
        return tracks_id["track_id"].to_list()

    def post_new(self):
        new_data_json = request.json
        self.model.add_new(new_data_json)
        tracks_id: pd.DataFrame = self.model.get_tracks_for_last_week()
        return tracks_id["track_id"].to_list()

    def run(self, *args, **kwargs):
        self.app.run(*args, **kwargs)


if __name__ == '__main__':
    app = MicroSerwis()
    app.run(host="0.0.0.0", port=8000)
