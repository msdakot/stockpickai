import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb

# loading local csv-file
results_df=pd.read_csv('results_first.csv')

df=pd.read_csv('AAPL_prices.csv')
df['datadate']=pd.to_datetime(df['datadate'])

cols = ['stock_name','model_acc','model_pred_performance','comp_performance']

#showing file with streamlit
st.header('Stock Pick Ai Top Picks')
st.subheader("Stocks that would outperform S&P500")

month_list = results_df['months'].unique().tolist()

location_selector = st.selectbox(
    "Select a month",
    month_list 
)
st.dataframe(results_df.head(5))

# plot the csv-file
fig = plt.figure(figsize=(20,5))
sb.set(style="darkgrid")
sb.lineplot(x=df['datadate'], y=df['prccd'],label='Apple')
st.pyplot(fig)