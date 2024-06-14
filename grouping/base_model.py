from .limits import TIME_LIMIT
import pandas as pd
from random import randint


def base_model(data: pd.DataFrame) -> pd.DataFrame:     # [track_id, group]
    track_times = data[['id_x', 'duration_ms']]

    # mieszanie wierszy
    track_times.sample(frac=1).reset_index(drop=True)
    current_group = 0
    group_sum = 0

    result = []

    for index, row in track_times.iterrows():
        result.append((row['id_x'], current_group))
        group_sum += row['duration_ms']
        if group_sum >= TIME_LIMIT:        # godzina
            group_sum = 0
            current_group += 1

    # ostatnia grupa mniej niż godzina
    if group_sum < TIME_LIMIT:
        for i in range(len(result)):
            if result[-1-i][1] == current_group:
                result[-1-i] = (result[-1-i][0], randint(0, current_group-1))
            else:
                break

    result_df = pd.DataFrame(result, columns=['id', 'group'])
    return result_df
