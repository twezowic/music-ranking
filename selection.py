from grouping.advanced_model import advanced_model
from prepare_data import prepare_grouping
import pandas as pd
import warnings

warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)


def calculate_mean_variance(data, group='group_x'):
    group_variance = data.groupby(group).var().mean(axis=1)
    mean_variance = sum(group_variance) / len(group_variance)
    return mean_variance


selected_columns = [
    'explicit', 'release_date', 'danceability', 'energy', 'key',
    'loudness', 'speechiness', 'acousticness', 'instrumentalness',
    'liveness', 'valence', 'tempo'
]

prepared_data = prepare_grouping()

grouped_data = advanced_model(prepared_data)
merged_data = prepared_data.merge(grouped_data, on='id_x')


attributes = selected_columns.copy()

initial_mean_variance = calculate_mean_variance(merged_data[selected_columns + ['group_x']])
results = [{'Removed Attribute': 'None', 'Mean Variance': initial_mean_variance}]

while len(attributes) > 1:
    worst_attribute = None
    worst_mean_variance = float('-inf')

    for attribute in attributes:
        temp_columns = attributes.copy()
        temp_columns.remove(attribute)

        prepared_data = prepare_grouping()
        temp_prepared_data = prepared_data[temp_columns + ['id_x', 'duration_ms']]
        temp_grouped_data = advanced_model(temp_prepared_data)
        temp_merged_data = prepared_data.merge(temp_grouped_data, on='id_x')

        current_mean_variance = calculate_mean_variance(temp_merged_data[selected_columns + ['group']], 'group')
        print(attribute, current_mean_variance)

        if current_mean_variance > worst_mean_variance:
            worst_mean_variance = current_mean_variance
            worst_attribute = attribute

    attributes.remove(worst_attribute)
    results.append({'Removed Attribute': worst_attribute, 'Mean Variance': worst_mean_variance})

results_df = pd.DataFrame(results)
results_df.to_csv('backward_selection.csv', index=False)
