from .limits import TIME_LIMIT
import pandas as pd
from scipy.cluster.hierarchy import linkage


def check_less(data, min_duration=TIME_LIMIT):
    result = []
    for group in data['group'].unique():
        group_data = data[data['group'] == group]
        if group_data['duration_ms'].sum() < min_duration:
            result.append(group)
    return result


def check_all_more_than_hour(data: pd.DataFrame) -> bool:
    return all(data.groupby(by=['group'])['duration_ms'].sum() > TIME_LIMIT)


def merge_clusters(data):
    params = data.drop(columns=['duration_ms', 'id_x'])
    clustering = linkage(params, method='ward')

    n_samples = data.shape[0]
    data['group'] = range(n_samples)

    for i, (cluster1, cluster2, _, _) in enumerate(clustering):
        data.loc[data['group'] == cluster1, 'group'] = n_samples + i
        data.loc[data['group'] == cluster2, 'group'] = n_samples + i
        if check_all_more_than_hour(data):
            break

    return data


def advanced_model(data: pd.DataFrame) -> pd.DataFrame:     # [track_id, group]
    result = merge_clusters(data)

    less_hour = check_less(result)

    result = data[~data['group'].isin(less_hour)]

    return result[['id_x', 'group']]
