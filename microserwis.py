from flask import Flask, request, jsonify
from Ranking import Ranking
import pandas as pd


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

    def get_playlists(self):
        tracks_id: pd.DataFrame = self.model.get_tracks_for_last_week()["track_id"].to_list()
        return tracks_id

    def run(self, *args, **kwargs):
        self.app.run(*args, **kwargs)


if __name__ == '__main__':
    app = MicroSerwis()
    app.run(host="0.0.0.0", port=8000)
