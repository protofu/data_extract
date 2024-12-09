import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output

app = Dash(__name__)

# 데이터 불러오기
df = pd.read_excel('sample_data.xlsx')
df = df.groupby(['State', 'ANSI', 'Affected by', 'Year', 'state_code'])[['Pct of Colonies Impacted']].mean()
df.reset_index(inplace=True)
df.head()
print(df)