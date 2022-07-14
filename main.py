import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb

# loading local csv-file
df=pd.read_csv('AAPL_prices.csv')
df['datadate']=pd.to_datetime(df['datadate'])

#showing file with streamlit
st.header('Sample streamlit dashboard')
st.subheader('share price of Apple')
st.dataframe(df.head(5))

# plot the csv-file
fig = plt.figure(figsize=(20,5))
sb.set(style="darkgrid")
sb.lineplot(x=df['datadate'], y=df['prccd'],label='Apple')
st.pyplot(fig)