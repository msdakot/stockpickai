import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.io as pio
import seaborn as sb
import s3fs
import boto3
import botocore


ACCESS_KEY = st.secrets['AWS_ACCESS_KEY_ID']
SECRET_KEY = st.secrets['AWS_SECRET_ACCESS_KEY']
BUCKET = 'w210-wrds-data'

S3_READ_TIMEOUT = 180
S3_MAX_ATTEMPTS = 5

s3_config = botocore.config.Config(read_timeout=S3_READ_TIMEOUT,retries={'max_attempts': S3_MAX_ATTEMPTS})


s3_client = boto3.session.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,).client('s3',config=s3_config)


# def get_latest(aws_bucket, directory):
    
#     result = s3_client.list_objects_v2(Bucket=aws_bucket,Prefix= (directory+"/"),Delimiter='/')
#     dir_ls = []
#     for obj in result.get('CommonPrefixes', []):
#         path = obj['Prefix']
#         dir_ls.append(path.rsplit('/')[-2])
#     return max(dir_ls)

# def s3_parquet_object_as_df(aws_bucket, directory, dataset):
#     latest_version = get_latest(aws_bucket, directory)
#     aws_object_path = f's3://{aws_bucket}/{directory}/{latest_version}/{dataset}.csv'
    
#     return pd.read_parquet(aws_object_path)

# loading local csv-file
top_picks_df=pd.read_csv('data/top_picks_information.csv')
model_df=pd.read_csv('data/model-period-10-year-120-cycles.csv')
chart_df = pd.read_csv('data/average_chart.csv').reset_index()

#processing 
top_picks_df['start_date']=pd.to_datetime(top_picks_df['start_date'])
top_picks_df['month'] = top_picks_df['start_date'].dt.to_period('M')


#showing file with streamlit
st.header('Stock Pick Ai Top Picks')
st.subheader("Stocks that would outperform S&P500")

month_list = top_picks_df['month'].unique().tolist()

month_selector = st.selectbox(
    "Select a month period",
    month_list 
)

col_to_print = {'month':'Selected Month Period',
                'top_pick': 'StockPick Stocks',
                'full_name':'StockPick Stock Name',
                'probability': 'Probability to outperform S&P',
                'model_accuracy': 'StockPick Model Accuracy'}
filter_pick_df = top_picks_df[top_picks_df['month'] == month_selector].copy().rename(columns=col_to_print )

st.dataframe(filter_pick_df[list(col_to_print.values())].head(5))


stock_list_to_plot =  filter_pick_df['StockPick Stocks'].tolist()

# plot the csv-file
#processing 
chart_df['datadate']=pd.to_datetime(chart_df['datadate'])

pio.templates.default = "plotly_white"

fig = go.Figure()
fig.add_trace(go.Scatter(x=chart_df['datadate'], y=chart_df['Stocks Average Return'],
                    mode='lines',
                    line_color='#fe7062',
                    name='StockPick Avearge Return'))

fig.add_trace(go.Scatter(x=chart_df['datadate'], y=chart_df['S and P Average Return'],
                    mode='lines',
                    line_color='#456067',
                    name='S&P Average Return'))

fig.update_layout(legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="left",
    x=0.01
))

# fig.show()

st.plotly_chart(fig)