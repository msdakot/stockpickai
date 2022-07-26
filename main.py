import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.io as pio
import seaborn as sb

# loading local csv-file
top_picks_df=pd.read_csv('data/top_picks_information.csv')
model_df=pd.read_csv('data/model-period-7-year-60-cycles.csv')
chart_df = pd.read_csv('data/model-period-7-year-60-cycles.csv')

#processing 
top_picks_df['start_date']=pd.to_datetime(top_picks_df['start_date'])
top_picks_df['month'] = top_picks_df['start_date'].dt.to_period('M')


#showing file with streamlit
st.header('Stock Pick Ai Top Picks')
st.subheader("Stocks that would outperform S&P500")

month_list = results_df['month'].unique().tolist()

month_selector = st.selectbox(
    "Select a month period",
    month_list 
)

col_to_print = {'month':'Selected Month Period',
                'top_pick': 'StockPick Stocks',
                'probability': 'Probability to outperform S&P',
                'model_accuracy': 'StockPick Model Accuracy'}
filter_pick_df = top_picks_df[top_picks_df['month'] == month_selector].copy().rename(columns=col_to_print )

st.dataframe(filter_pick_df[list(col_to_print.keys())].head(5))

# plot the csv-file
pio.templates.default = "plotly_white"

fig = go.Figure()
fig.add_trace(go.Scatter(x=chart_df['date'], y=chart_df['Stocks Average Return'],
                    mode='lines',
                    line_color='#fe7062',
                    name='StockPick Avearge Return'))

fig.add_trace(go.Scatter(x=chart_df['date'], y=chart_df['S and P Average Return'],
                    mode='lines',
                    line_color='#456067',
                    name='S&P Average Return'))

fig.update_layout(legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="left",
    x=0.01
))

fig.show()