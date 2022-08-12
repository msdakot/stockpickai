import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.io as pio
import s3fs
import s3finance as s3c
import boto3
import botocore

ACCESS_KEY = ''
SECRET_KEY = ''
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

print(top_picks_df.head())