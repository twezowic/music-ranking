from prepare_data import prepare_grouping
from grouping.advanced_model import advanced_model
from grouping.base_model import base_model
import numpy as np
import pandas as pd


def AB_experiment(session_base, session_adv, tracks, artists):
    result = []

    data = prepare_grouping(tracks=tracks, artists=artists,
                            ranking_result=session_base,
                            with_onehotencoding=True)

    params = data.drop(columns=['id_x', 'duration_ms'])

    base = base_model(data)
    labels = base['group']
    group_num_base = len(np.unique(labels, return_counts=True)[0])

    params = params.reset_index(drop=True)
    labels = labels.reset_index(drop=True)
    bas = params.assign(group=labels)

    mean_variance_group_base = bas.groupby('group').var().mean(axis=1)
    mean_variance_group_base = np.nansum(mean_variance_group_base) / len(mean_variance_group_base)

    result.append(f"Base: group_num: {group_num_base}, variance:{mean_variance_group_base}")

    data = prepare_grouping(tracks=tracks, artists=artists,
                            ranking_result=session_adv,
                            with_onehotencoding=True)

    params = data.drop(columns=['id_x', 'duration_ms'])

    advanced = advanced_model(data)
    labels = advanced['group']
    params_dropped = data[data['id_x'].isin(advanced['id_x'])].drop(columns=['id_x', 'duration_ms', 'group'])
    group_num_adv = len(np.unique(labels, return_counts=True)[0])

    adv = pd.concat([params_dropped, labels], axis=1)
    mean_variance_group_adv = adv.groupby('group').var().mean(axis=1)
    mean_variance_group_adv = sum(mean_variance_group_adv) / len(mean_variance_group_adv)

    result.append(f"Advanced: group_num: {group_num_adv}, variance:{mean_variance_group_adv}")

    return "\n".join(result)
