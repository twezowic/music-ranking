from Ranking import Ranking
from prepare_data import prepare_grouping
from grouping.advanced_model import advanced_model
from grouping.base_model import base_model


def main(is_base=True):
    r = Ranking(from_csv="a", weeks=3, limit=0.25)
    r.group_by_weeks()

    result = r.get_tracks_for_last_week()

    prepared_data = prepare_grouping(tracks='a', artists='a',
                                     ranking_result=result,
                                     with_onehotencoding=True)

    if is_base:
        return base_model(prepared_data)
    else:
        return advanced_model(prepared_data)
