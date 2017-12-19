import pandas as pd
import numpy as np
from datetime import datetime

file_name = 'chicago-community-areas.csv'
df = pd.read_csv(file_name, header=0)
print(df)