import pandas as pd
from logging import error, info

df = pd.read_csv('pvc_values.csv', delimiter="|")
output_df = df.loc[(df['percent_used']) >= 80]

for idx, row in output_df.iterrows():
    info(f"{(row['PVC'])} has {row['percent_used']} remaining")

if len(output_df) > (0):
    for idx, row in output_df.iterrows():
        error(f"{(row['PVC'])} is running out of space")
    raise ValueError("PVCs are low on space, please investigate on GCS")