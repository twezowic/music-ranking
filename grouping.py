import pandas as pd
from sklearn.cluster import AgglomerativeClustering

df = pd.read_csv("grouping.csv").to_numpy()

test = df[:100]