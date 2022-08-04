import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.io as pio
import seaborn as sb
import s3fs
import s3finance as s3c
import boto3
import botocore


ACCESS_KEY = st.secrets['AWS_ACCESS_KEY_ID']
SECRET_KEY = st.secrets['AWS_SECRET_ACCESS_KEY']
BUCKET = 'w210-wrds-data'

s3f = s3c.s3finance(BUCKET,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY)


def min_max_scaling(column):
    return(column - column.min())/(column.max()-column.min())

# toppick df
top_picks_df = s3f.gettoppickstable()
top_picks_df['start_date']=pd.to_datetime(top_picks_df['start_date'])
top_picks_df['month'] = top_picks_df['start_date'].dt.to_period('M')

#feature df
modelname = 'model-period-10-year-214-cycles-1659216789.930134-full'
model_pre_df = s3f.getmodelexplainability(modelname)
feature_vars = model_pre_df.columns
model_pre_df = model_pre_df.reset_index()
model_df  = pd.melt(model_pre_df, id_vars='datadate', value_vars=feature_vars)
model_df.columns = ['datadate','feature','value score']
model_desc_df = pd.read_csv('data/feature_definition.csv')
model_df = model_df.merge(model_desc_df, how='left', on='feature')

model_df['datadate'] = pd.to_datetime(model_df['datadate'])
model_df['month'] = model_df ['datadate'].dt.to_period('M')

#chart df
chart_df = s3f.getmonthlyavgchart().reset_index()
chart_df['datadate'] = pd.to_datetime(chart_df['datadate'])

#get unique months
month_list = top_picks_df['month'].unique().tolist()
#
#showing file with streamlit
st.header('StockPick Top Ranked Stocks')
st.subheader("Stocks that would outperform S&P500")

st.markdown(
    """
    
    <br><br/>
    Increase the Returns-On-Investment and expand your investing based knowledge whether a stock with outperform SP500 Index. 
    Start with our monthly top five runners, then explore why they are our top picks and make investment choices to match your risk appetite, click below.
    """
    , unsafe_allow_html=True)

month_selector = st.selectbox(
    "Select a month period",
    month_list 
)

#toppick filter
col_to_print = {'top_pick': 'StockPick Stocks',
                'full_name':'StockPick Stock Name',
                'start_date':'StockPick Buy Date',
                'end_date':'StockPick Sell Date',
                'probability': 'Probability to outperform S&P',
                'model_accuracy': 'StockPick Model Accuracy'}
filter_pick_df = top_picks_df[top_picks_df['month'] == month_selector].copy().rename(columns=col_to_print )

st.dataframe(filter_pick_df[list(col_to_print.values())].head(5))

#model feature importance filter

filter_model_df = model_df[model_df['month']==month_selector].copy()
filter_model_df['scaled value score'] = min_max_scaling(filter_model_df['value score'])
filter_model_df  = filter_model_df.sort_values(by='scaled value score', ascending=False)



st.markdown(""" <h1 style= = "font-family: Ubuntu, Helvetica; font-style: italic; font-size: 30px">
                 Allow our automation to make you look like an investing genius 
                 </h1> """, unsafe_allow_html=True)

st.markdown(
    """
    <p style = "font-family: Ubuntu, Helvetica">
    <br><br/>
    Our automated software helps optimize your investments by doing the work for you by picking stock collections based on multiple factors.
    Expand your investment strategy by exploring what's powering our model and stock collections. 
    Explore what financial features of the company or industry boost a stocks out-performance, what to be on the lookout for when investing longterm.
    </p>
    """
    , unsafe_allow_html=True)


#explanatory variables 
st.dataframe(filter_model_df.head(10))

text_desc,text_def = filter_model_df['feature description'].head(10),filter_model_df['feature definition'].head(10)

opening_html = '<div>'
closing_html = '</div>'

def flex_button_string(description_child,content_child):
    return (f'''
        <button type="button" class="collapsible">{description_child}</button>
        <div class="content">
        <p>{content_child}</p>
        </div>
    ''')

gallery_html = opening_html
for description_child,content_child in zip(text_desc,text_def):
    gallery_html += flex_button_string(description_child,content_child)
gallery_html += closing_html

st.markdown("""<link href="button.css" """, unsafe_allow_html=True)
st.markdown(gallery_html, unsafe_allow_html=True)


# stock_list_to_plot =  filter_pick_df['StockPick Stocks'].tolist()

# plot the csv-file 
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