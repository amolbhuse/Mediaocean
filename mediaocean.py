#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import requests
import datetime
import numpy as np
import pandas.io.sql as psql
import mysql.connector
import pandas as pd


# In[2]:


def get_theatre(api_key,startDate,zip_code,line_up_id,date_time):



    r = requests.get('http://data.tmsapi.com/v1.1/movies/showings?startDate='+startDate+'&zip='+zip_code+'&api_key='+api_key+'')

    response = r.json()

    df_theatre =pd.DataFrame()
    for  i in range(0,len(response)):
        try:
            Title =response[i]['title']
            Release_year=response[i]['releaseYear']
            #print(Release_year)
            Genre =response[i]['genres']
            Genres=', '.join(str(x) for x in Genre)
            #print(Genres)
            Description =response[i]['shortDescription']
            #print(Description)
            Theat =response[i]['showtimes']
            the_len= len(Theat)
            for j in range(0,the_len):
                Theatre =response[i]['showtimes'][j]['theatre']['name']
                data_list = [[Title,Release_year,Genres,Description,Theatre]]
                df_data = pd.DataFrame(data_list,columns=['Title','Release_year','Genres','Description','Theatre'])
                df_theatre =df_theatre.append(df_data)
            
                #print(Theatre)
        except:
            pass
    df_theatre.drop_duplicates(keep ='first',inplace=True)
    
    
    r = requests.get('http://data.tmsapi.com/v1.1/movies/airings?lineupId='+line_up_id+'&startDateTime='+date_time+'&api_key='+api_key+'')
    response = r.json()
    df_channel =pd.DataFrame()
    for  i in range(0,len(response)):
        try:
            Title =response[i]['program']['title']
            Release_year=response[i]['program']['releaseYear']
            #print(Release_year)
            Genre =response[i]['program']['genres']
            Genres=', '.join(str(x) for x in Genre)
            #print(Genres)
            Description =response[i]['program']['shortDescription']
            #print(Description)
            ch =response[i]['channels']
            channel=', '.join(str(x) for x in ch)
            
            data_list = [[Title,Release_year,Genres,Description,channel]]
            df_da = pd.DataFrame(data_list,columns=['Title','Release_year','Genres','Description','channel'])
            df_channel =df_channel.append(df_da)
            
                #print(Theatre)
        except:
            pass
    df_channel.drop_duplicates(keep ='first',inplace=True)
    return df_theatre,df_channel
        

    


# In[3]:


def get_top5():
    # Join both columns based on generes
    join = pd.merge(df_theatre, df_channel, how='inner', on=['Genres'])

    # Unique count movies group by Genres
    j1 =join[['Title_x','Title_y','Genres']].groupby('Genres').agg('nunique')
    j1['count'] = j1['Title_x']+j1['Title_y']
    j1 =j1[['count']]
    j1=j1.reset_index()
    join2= pd.merge(join, j1, how='inner', on=['Genres'])                                                          
    
    
    j_theatre = join2.drop_duplicates(subset=['Genres', 'Title_x', 'count'])
    j4 =j_theatre.groupby(['Genres','count'])['Title_x'].apply(lambda x: ','.join(x)).reset_index()
    j4.rename(columns = {'Title_x':'Theatre_movie'}, inplace = True)
    j_channel = join2.drop_duplicates(subset=['Genres', 'Title_y', 'count'])
    j5 =j_channel.groupby(['Genres','count'])['Title_y'].apply(lambda x: ','.join(x)).reset_index()
    j5.rename(columns = {'Title_y':'Channel_movie'}, inplace = True)
    
    final = pd.merge(j4, j5, how='inner', on=['Genres'])
    final1=final[['Genres','Theatre_movie','Channel_movie','count_x']]
    final1=final1.sort_values('count_x', ascending=False)
    return final1.head()


# ## Call function 1

# In[5]:


df_theatre,df_channel =get_theatre('5tdas6bwy3rx9fz4aq65u5zd','2020-11-08','78701','USA-TX42500-X','2020-11-07T09:30Z')


# ## Export data at sqlalchemy Database

# In[6]:


from sqlalchemy import create_engine
import sqlite3
conn = sqlite3.connect('db.sqlite3')

df_theatre.to_sql("theatres", conn, if_exists="replace")
df_channel.to_sql("channels", conn, if_exists="replace")


# ## Call Function2

# In[11]:


get_top5()


# In[ ]:




