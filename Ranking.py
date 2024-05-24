import pandas as pd
import json
import numpy as np
import datetime
from tqdm import tqdm
from matplotlib import pyplot as plt


def jsonl2df(path):
    with open(path) as fh:
        lines = fh.read().splitlines()

    df_inter = pd.DataFrame(lines)
    df_inter.columns = ["json_element"]

    df_inter["json_element"].apply(json.loads)

    return pd.json_normalize(df_inter["json_element"].apply(json.loads))


def increase_week(args, *, by=1):
    year, week = int(args[0]), int(args[1])
    first_date_of_year = datetime.date(year, 1, 1)
    if first_date_of_year.isocalendar()[1] != 1:
        first_date_of_year += datetime.timedelta(
            days=(7 - first_date_of_year.weekday())
        )

    current_date = first_date_of_year + datetime.timedelta(weeks=week - 1)
    new_date = current_date + datetime.timedelta(weeks=by)
    new_year, new_week, _ = new_date.isocalendar()
    return new_year, new_week


class Ranking:

    __slots__ = [
        "sessions",
        "sessions_popularity",
        "sessions_popularity_per_weeks",
        "week_count",
        "limit",
    ]

    def __init__(self, *, path=None, from_csv=None, weeks=1, limit=1) -> None:
        """
        Making ranking for every period and 

        Parameters
        ----------
        path : str
            Path to jsonl file to read sessions and calculate popularity. First time you have to run with path.
        from_csv : str
            Name of csv file to load previously converted data.
        weeks : int
            Weeks as the period of time in which popularity is calculated.
        limit : float
            From 0 to 1, part of tracks_id which will be returned.
        """
        self.sessions = None
        try:
            self.sessions_popularity = pd.read_csv(from_csv)
        except Exception:
            print(f"Couldn't find {from_csv}")
            self.sessions = jsonl2df(path).dropna()
            self.sessions_popularity = self.count_popularity()
        self.sessions_popularity["week_tuple"] = list(
            zip(self.sessions_popularity["year"], self.sessions_popularity["week"])
        )
        self.sessions_popularity_per_weeks = {}
        self.week_count = weeks
        self.limit = limit

    def __clean(self) -> pd.DataFrame:
        sessions_cleaned = self.sessions[
            self.sessions["event_type"].isin(["play", "skip"])
        ].copy()
        sessions_cleaned["timestamp"] = pd.to_datetime(
            sessions_cleaned["timestamp"], errors="coerce"
        )
        sessions_cleaned = sessions_cleaned.sort_values(by=["session_id", "timestamp"])
        sessions_cleaned.loc[:, "popularity"] = (
            sessions_cleaned["event_type"]
            .map({"play": 1, "skip": 0})
            .astype(np.float64)
        )
        return sessions_cleaned

    def to_csv(self, filename: str) -> None:
        self.sessions_popularity = self.sessions_popularity.drop(columns=["week_tuple"])
        self.sessions_popularity.to_csv(filename, index=False)

    def _get_first_week(self) -> tuple[int, int]:
        year, week = (
            self.sessions_popularity.iloc[0]["year"],
            self.sessions_popularity.iloc[0]["week"],
        )
        return year, week

    def count_popularity(self) -> pd.DataFrame:
        epsilon = pd.Timedelta(
            minutes=1
        )
        sessions_cleaned = self.__clean()
        len_cleaned_sessions = len(sessions_cleaned)
        for idx, (_, row) in tqdm(enumerate(sessions_cleaned.iterrows())):
            if len_cleaned_sessions - 1 == idx:
                break
            next_row = sessions_cleaned.iloc[idx + 1]
            if row["event_type"] == "play" and next_row["event_type"] == "skip":
                if row["session_id"] == next_row["session_id"]:
                    diff_time = (
                        sessions_cleaned.iloc[idx + 1]["timestamp"] - row["timestamp"]
                    )
                    if diff_time < epsilon:
                        sessions_cleaned.at[next_row.name, "popularity"] = -1.5
        isocalendar_df = sessions_cleaned["timestamp"].dt.isocalendar()
        sessions_cleaned.loc[:, "week"] = isocalendar_df["week"]
        sessions_cleaned.loc[:, "year"] = isocalendar_df["year"]

        sessions_popularity = (
            sessions_cleaned.groupby(["year", "week", "track_id"])
            .agg({"popularity": "sum"})
            .reset_index()
        )
        sessions_popularity = sessions_popularity.sort_values(
            by=["year", "week", "popularity"], ascending=[True, True, False]
        )
        return sessions_popularity

    def group_by_weeks(self) -> None:
        lenght = len(self.sessions_popularity.groupby(["year", "week"]))
        next_weeks = [self._get_first_week()]
        for _ in range(self.week_count - 1):
            next_weeks.append(increase_week(next_weeks[-1]))

        for _ in tqdm(range(lenght - self.week_count)):
            selected_rows = self.sessions_popularity[
                self.sessions_popularity["week_tuple"].isin(next_weeks)
            ]
            week_from = selected_rows.iloc[0]["week_tuple"]
            week_to = selected_rows.iloc[-1]["week_tuple"]
            count = (
                selected_rows.groupby("track_id")["popularity"]
                .sum()
                .reset_index(name="popularity")
                .sort_values(by="popularity", ascending=False)
            )
            self.sessions_popularity_per_weeks[(week_from, week_to)] = count
            next_weeks.append(increase_week(next_weeks[-1]))
            next_weeks = next_weeks[1:]

    def get_frame(self, week_from: tuple, *, limit=None, random=False) -> pd.DataFrame:
        week_to = increase_week(week_from, by=self.week_count - 1)
        df: pd.DataFrame = self.sessions_popularity_per_weeks.get((week_from, week_to))
        if limit is None:
            limit = self.limit
        num_rows = int(len(df) * limit // 1)
        if not random:
            df = df.iloc[:num_rows]
        else:
            df = df.sample(num_rows)
        df = df[df["popularity"] > 0]
        return df[["track_id"]].copy()

    def get_tracks_for_week(self, week_from: tuple) -> pd.DataFrame:
        selected_rows = self.sessions_popularity[
            (self.sessions_popularity["year"] == week_from[0])
            & (self.sessions_popularity["week"] == week_from[1])
        ]
        tracks = selected_rows[["track_id"]].copy()
        return tracks

    def make_test(self, date: tuple, *, random=False) -> np.float64:
        tracks_for_past_weeks = self.get_frame(date, random=random)
        future_week = increase_week(date, by=self.week_count)
        tracks_for_test = self.get_tracks_for_week(future_week)
        merged_df = tracks_for_test.merge(
            tracks_for_past_weeks, how="inner", on="track_id"
        )
        id_counts = len(merged_df) / len(tracks_for_past_weeks) * 100
        return round(id_counts, 3)

    def make_test_for_every_frame(self, random=False):
        date = self._get_first_week()
        to_print = []
        for _ in tqdm(range(len(self.sessions_popularity_per_weeks.keys()))):
            to_print.append(
                f"Ranking z tygodni: {date}-{increase_week(date, by=self.week_count-1)}. \
Ilość trafień % w tygodniu {increase_week(date, by=self.week_count)}: {self.make_test(date, random=random)}%"
            )
            date = increase_week(date)
        for line in to_print:
            print(line)

    def compare_to_basic(self):
        date = self._get_first_week()
        to_print = ["Period:    Which is better?"]
        for _ in tqdm(range(len(self.sessions_popularity_per_weeks.keys()))):
            basic = self.make_test(date, random=True)
            extended = self.make_test(date, random=False)
            to_print.append(
                f"{date}-{increase_week(date, by=self.week_count-1)}: \
    {'Basic' if basic > extended else 'Extended'}"
            )
            date = increase_week(date)
        for line in to_print:
            print(line)

    def make_plot(self, track_id) -> None:
        year, week = (
            self.sessions_popularity.iloc[0]["year"],
            self.sessions_popularity.iloc[0]["week"],
        )
        week_from = (year, week)
        popularity_in_time = []
        weeks = []
        for _ in range(len(self.sessions_popularity_per_weeks.keys())):
            week_to = increase_week(week_from, by=self.week_count - 1)
            frame: pd.DataFrame = self.sessions_popularity_per_weeks.get((week_from, week_to))
            x: pd.DataFrame = frame[frame["track_id"] == track_id]
            if not x.empty:
                popularity_in_time.append(x.iloc[0]["popularity"])
                weeks.append((week_from, week_to))
            week_from = increase_week(week_from)
        plt.figure(figsize=(10, 6))
        plt.plot(range(len(weeks)), popularity_in_time, marker='o', linestyle='-', color='r')
        plt.xlabel('Tygodnie')
        plt.ylabel('Popularność')
        plt.title(f'Popularność dla {track_id}')
        plt.xticks(range(len(weeks)), [f'{f[0]}-{f[1]}, {to[0]}-{to[1]}' for f, to in weeks], rotation=45)
        plt.tight_layout()
        plt.show()

    def get_tracks_for_last_week(self) -> pd.DataFrame:
        last_week = list(self.sessions_popularity_per_weeks.keys())[-1][0]
        tracks = self.get_frame(last_week)
        return tracks


if __name__ == "__main__":
    r = Ranking("datav2/sessions.jsonl", weeks=3, limit=1)
    r.group_by_weeks()
    r.make_test_for_every_frame()
