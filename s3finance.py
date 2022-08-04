#
# This is the file that we use to access data from S3
#

#
# This file has code to pull stocks, and summary data from an S3 bucket and the find_stocks function can be put into
# any code to pull a paricular stock that merges fundamental and price data as we have done in our
# early prototype.
#

import time
import numpy as np
import pandas as pd
from datetime import datetime
import s3fs
import os
import pprint
import boto3
import re
from io import BytesIO
import tempfile
import joblib

# class to get all the data for stock and summaries
# get 

class s3finance:
    def __init__(self, bucket,usecsv=False,usepkl=False,useparquet=True,version=None,aws_access_key_id=None,aws_secret_access_key=None):
        self.bucket = bucket
        if aws_access_key_id is None:
            self.s3 = boto3.client('s3')
        else:
            self.s3 = boto3.client('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
        self.paginator = self.s3.get_paginator('list_objects_v2')
        # set timestamp
        dt = datetime.now()
        self.ts = datetime.timestamp(dt)
        self.usepkl = usepkl
        self.useparquet = useparquet
        self.usecsv = usecsv
        self.version = version
        self.currentversion = None
    
    def settimestamp():
        # set timestamp
        dt = datetime.now()
        self.ts = datetime.timestamp(dt)
        
    def getobject(self, prefix, keycontent):
        if self.version is not None:
            prefix = prefix + '/' + self.version
        
        page_iterator = self.paginator.paginate(
            Bucket=self.bucket,
            Prefix=prefix+'/')

        #print(f"Contents[?contains(Key, `/{keycontent}`)][]")
        object_filtered_iterator = page_iterator.search(f"Contents[?contains(Key, `/{keycontent}`)][]")
            
        obj=None
        objpkl=None
        objparquet=None
        for page in object_filtered_iterator:
            #print(page)
            if self.useparquet and '.parquet' in page['Key']:
                if objparquet is None:
                    objparquet = page
                    continue
                if page['LastModified'] > objparquet['LastModified']:
                    objparquet = page
                    continue
            if self.usepkl and '.pkl' in page['Key']:
                if objpkl is None:
                    objpkl = page
                    continue
                if page['LastModified'] > objpkl['LastModified']:
                    objpkl = page
                    continue
            if '.csv' in page['Key']:
                if obj is None:
                    obj = page
                    continue
                if page['LastModified'] > obj['LastModified']:
                    obj = page
                    continue                    

            #print(page)

        if obj is None and objpkl is None and objparquet is None:
            return None


        if self.useparquet and objparquet is not None:
            obj_df = pd.read_parquet(f"s3://{self.bucket}/{objparquet['Key']}")
            self.currentversion = objparquet['Key'].split('/')[1]
            return obj_df

        if self.usepkl and objpkl is not None:
            #print(f"s3://{self.bucket}/{objpkl['Key']}")
            obj_df = pd.read_pickle(f"s3://{self.bucket}/{objpkl['Key']}")
            self.currentversion = objpkl['Key'].split('/')[1]
            return obj_df

        obj_df = pd.read_csv(f"s3://{self.bucket}/{obj['Key']}")            
        self.currentversion = obj['Key'].split('/')[1]
        return obj_df

    def putobject(self, df, prefix, keycontent):
        if self.version is not None:
            version =  self.version
        else:
            version = self.ts
        storage_opts = {'s3_additional_kwargs':
                                {'ACL': 'bucket-owner-full-control'}}
        if self.usecsv:
            df.to_csv(f"s3://{self.bucket}/{prefix}/{version}/{keycontent}.csv", index = False, 
                      storage_options=storage_opts) # ,quoting=csv.QUOTE_NONNUMERIC)
        # df.to_csv(f"s3://{self.bucket}/{prefix}/{version}/{keycontent}.csv", index = False) # ,quoting=csv.QUOTE_NONNUMERIC)
        if self.usepkl:
            df.to_pickle(f"s3://{self.bucket}/{prefix}/{version}/{keycontent}.pkl",storage_options=storage_opts)
        if self.useparquet:
            df.to_parquet(f"s3://{self.bucket}/{prefix}/{version}/{keycontent}.parquet",storage_options=storage_opts)
        return df

    def getbasefile(self, file):
        fund_df = self.getobject("base", file)
        return fund_df

    def putbasefile(self, df,file):
        fund_df = self.putobject(df,"base", file)
        return fund_df

    def	getlatest(self):
        df = self.getbasefile('active_version')
        return df['latest'][0]

    def setlatest(self):
        data = [self.ts]
        df = pd.DataFrame(data, columns=['latest'])
        self.putbasefile(df,'active_version')
        return df

    def getstockfundamentals(self, stock):
        fund_df = self.getobject("fundamentals", f"{stock}_")
        return fund_df

    def putstockfundamentals(self, df,stock):
        fund_df = self.putobject(df,"fundamentals", f"{stock}_fundamentals")
        return fund_df

    def getstockprices(self, stock):
        price_df = self.getobject("prices", f"{stock}_")
        return price_df

    def putstockprices(self, df, stock):
        price_df = self.putobject(df, "prices", f"{stock}_prices")
        return price_df


    def getstockquarter(self, stock):
        price_df = self.getobject("quarter", f"{stock}_")
        return price_df

    def putstockquarter(self, df, stock):
        price_df = self.putobject(df, f"quarter", f"{stock}_quarter")
        return price_df

    def getstockannual(self, stock):
        price_df = self.getobject("annual", f"{stock}_")
        return price_df

    def putstockannual(self, df, stock):
        price_df = self.putobject(df, f"annual", f"{stock}_annual")
        return price_df

    
    def getstockmerged(self, stock):
        price_df = self.getstockprices(stock)
        fund_df = self.getstockfundamentals(stock)
        join_df = self.getobject("join", f"{stock}_")    
        return join_df,fund_df,price_df

    def putstockmerged(self, df, stock):
        join_df = self.putobject(df, f"join", f"{stock}_join")      
        return join_df

    def getsummaries(self, summary):
        summary_df = self.getobject("summaries", f"{summary}")
        return summary_df


    def putsummaries(self, df, summary):
        summary_df = self.putobject(df, "summaries", f"{summary}")
        return summary_df


    def getrawsummaries(self, summary):
        summary_df = self.getobject("rawsummaries", f"{summary}")
        return summary_df

    def putrawsummaries(self, df, summary):
        summary_df = self.putobject(df,"rawsummaries", f"{summary}")
        return summary_df

    def getfactset(self, filename):
        factset_df = self.getobject("factset", f"{filename}")

        return factset_df
    
    def getbase(self, filename):
        f = BytesIO()
        filevalues_df = self.s3.download_fileobj(self.bucket, filename, f)

    def getrawsummaryfund(self):
        return self.getrawsummaries("summary_fundamentals")

    def putrawsummaryfund(self,df):
        return self.putrawsummaries(df,"summary_fundamentals")

    def getrawsummaryprices(self):
        return self.getrawsummaries("summary_prices")

    def putrawsummaryprices(self,df):
        return self.putrawsummaries(df,"summary_prices")

    def getrawsummaryjoinpricesfund(self):
        return self.getrawsummaries("summary_join")

    def putrawsummaryjoinpricesfund(self,df):
        return self.putrawsummaries(df,"summary_join")
    
    def getrawsummaryquarter(self):
        return self.getrawsummaries("summary_quarter")

    def putrawsummaryquarter(self,df):
        return self.putrawsummaries(df,"summary_quarter")

    def getrawsummaryannual(self):
        return self.getrawsummaries("summary_annual")

    def putrawsummaryannual(self,df):
        return self.putrawsummaries(df,"summary_annual")

    def getsummaryfund(self):
        return self.getsummaries("summary_fundamentals")

    def putsummaryfund(self,df):
        return self.putsummaries(df,"summary_fundamentals")

    def getsummaryprices(self):
        return self.getsummaries("summary_prices")

    def putsummaryprices(self,df):
        return self.putsummaries(df,"summary_prices")

    def getsummaryjoinpricesfund(self):
        return self.getsummaries("summary_join")

    def putsummaryjoinpricesfund(self,df):
        return self.putsummaries(df, "summary_join")

    def getsummaryquarter(self):
        return self.getsummaries("summary_quarter")

    def putsummaryquarter(self,df):
        return self.putsummaries(df,"summary_quarter")
    
    def getsummaryannual(self):
        return self.getsummaries("summary_annual")
    
    def putsummaryannual(self,df):
        return self.putsummaries(df,"summary_annual")

    def getsummaryquarter_annual(self):
        return self.getsummaries("summary_quarterannual")
    
    def putsummaryquarter_annual(self,df):
        return self.putsummaries(df,"summary_quarterannual")

    def getsummaryallratioprices(self):
        return self.getsummaries("all_ratio_prices")

    def putsummaryallratioprices(self,df):
        return self.putsummaries(df,"all_ratio_prices")

    def getsummaryallratiopricesfinancial(self):
        return self.getsummaries("all_ratios_prices_financial_statements")

    def putsummaryallratiopricesfinancial(self,df):
        return self.putsummaries(df,"all_ratios_prices_financial_statements")

    def getcusipmapping(self):
        return self.getbasefile("cusip_relationship")

    def putcusipmapping(self,df):
        return self.putbasefile(df,"cusip_relationship")

    def getmainpagechart(self):
        return self.getbasefile("main_page_chart")

    def putmainpagechart(self,df):
        return self.putbasefile(df,"main_page_chart")

    def getmonthlyavgchart(self):
        return self.getbasefile("average_chart")

    def putmonthlyavgchart(self,df):
        return self.putbasefile(df,"average_chart")

    def gettoppickstable(self):
        return self.getbasefile("top_picks_information")

    def puttoppickstable(self,df):
        return self.putbasefile(df,"top_picks_information")

    def getstockpicksindex(self):
        return self.getbasefile("stockpicks_index")

    def putstockpicksindex(self,df):
        return self.putbasefile(df,"stockpicks_index")

    def getmodelexplainability(self,model_name):
        return self.getbasefile(f"model_explainability/{model_name}")

    def putmodelexplainability(self,df,model_name):
        return self.putbasefile(df,f"model_explainability/{model_name}")
    
    def getmodeldata(self,model_name):
        filename = f"modeldata/{model_name}"
        print(filename)
        model = None
        with BytesIO() as data:
            self.s3.download_fileobj(self.bucket, filename, data)
            data.seek(0)    # move back to the beginning after writing
            model = joblib.load(data)
        return model

    def putmodeldata(self,model,model_name):
        bucket_key = f"modeldata/{model_name}"
        print(self.bucket, bucket_key)
        with BytesIO() as data:
            joblib.dump(model,data)
            data.seek(0)
            self.s3.put_object(Body=data.read(), Bucket=self.bucket, Key=bucket_key)
        return model
