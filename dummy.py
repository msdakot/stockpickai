import pandas as pd
# import matplotlib.pyplot as plt
# import plotly.graph_objects as go
# import plotly.io as pio
# import seaborn as sb
import s3fs
# import boto3
# import botocore
import s3finance as s3c


ACCESS_KEY = 'AKIA3FT37EEL756GE5VY'
SECRET_KEY = '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'
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

model_df.to_csv('model_df.csv')

#chart df
chart_df = s3f.getmonthlyavgchart().reset_index()
chart_df['datadate']=pd.to_datetime(chart_df['datadate'])


#month choice
month_selector = top_picks_df['month'][0] 


filter_model_df = model_df[model_df['month']==month_selector].copy()

filter_model_df['scaled value score'] = min_max_scaling(filter_model_df['value score'])
filter_model_df  = filter_model_df.sort_values(by='scaled value score', ascending=False)


#html

text_desc,text_def = filter_model_df['feature description'].head(10),filter_model_df['feature definition'].head(10)

opening_html = '<div style=display:flex;flex-wrap:wrap>'
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


#print statements
print(top_picks_df.head())
print(model_df.head())

print(filter_model_df.sort_values(by='scaled value score', ascending=False).head())
print(chart_df.head())

print(gallery_html)