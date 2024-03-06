import googleapiclient.discovery
import pymongo
import psycopg2
import pandas as pd
import streamlit as sl

api_service_name = "youtube"
api_version = "v3"
api_key='AIzaSyBJ5nl-6uIwq2GlAy6nRJoH9C-Utpu6raw'
youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=api_key)

client=pymongo.MongoClient("mongodb://localhost:27017")
database=client["YouTube_data_harvesting"]

#---------------------------------------------------------------------------------------------------------------#

def get_channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)

    response = request.execute()
    
    for i in response['items']:
        data=dict(channel_name=i['snippet']['title'],channel_id=i['id'],
              subscriber_count=i['statistics']['subscriberCount'],
              total_videos =i["statistics"]["videoCount"],
             channel_views=i['statistics']['viewCount'],
             channel_discription=i['snippet']['description'],
             playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
    
    return data

def get_videos_ids(channel_id):
        videos_ids=[]
        np_token=None
        response = youtube.channels().list(part="contentDetails",
                                           id=channel_id).execute()
        playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        while True:
            response= youtube.playlistItems().list(
                    part="snippet",
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=np_token).execute()

            for i in range(len(response['items'])):
                    videos_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])

            np_token=response.get('nextPageToken') 

            if np_token is None:
                break
        return videos_ids

def get_video_data(videos_ids):
        videos_data=[]
        for i in videos_ids:
                response=youtube.videos().list(part='snippet,contentDetails,statistics',
                                         id=i).execute()
                for j in response['items']:
                    data=dict(channel_name=j['snippet']['channelTitle'],
                         video_id=j['id'],
                          video_name=j['snippet']['title'],
                         video_description=j['snippet']['description'],
                         tags=j['snippet'].get('tags'),
                         published_at=j['snippet']['publishedAt'],
                         view_count=j['statistics']['viewCount'],
                         like_count=j['statistics'].get('likeCount'),
                         favourite_count=j['statistics']['favoriteCount'],
                         comment_count=j['statistics'].get('commentCount'),
                         duration=j['contentDetails']['duration'],
                         thumbnails=j['snippet']['thumbnails']['default']['url'],
                         caption_status=j['contentDetails']['caption'])
                videos_data.append(data)
        return videos_data

def get_comment_data(video_id):
    comment_data=[]   
    try:
         for i in video_id:
                response=youtube.commentThreads().list(part='snippet',
                                                                    videoId=i,
                                                                    maxResults=50).execute()
                for j in response['items']:
                            data=dict(comment_id=j['snippet']['topLevelComment']['id'],
                                      video_id=j["snippet"]["videoId"],
                                                 comment_text=j['snippet']['topLevelComment']['snippet']['textDisplay'],
                                                 comment_author=j['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                                 comment_publishedat=j['snippet']['topLevelComment']['snippet']['publishedAt'])
                            comment_data.append(data)
    except:
        pass
    return comment_data

def get_playlist_data(channel_id):
        np_token= None
        playlist_data=[]
        while True:
                response=youtube.playlists().list(part='contentDetails,snippet',
                                                  channelId=channel_id,
                                                 maxResults=50,
                                                  pageToken=np_token).execute()
                for i in response['items']:
                    data=dict(playlist_id=i['id'],
                             channel_id=i['snippet']['channelId'],
                             channel_name=i['snippet']['channelTitle'],
                             playlist_name=i['snippet']['title'])
                    playlist_data.append(data)
                np_token=response.get('nextPageToken')
                if np_token is None:
                            break
        return playlist_data

def channel_datas(channel_id):
    channel_data=get_channel_data(channel_id)
    playlist_data=get_playlist_data(channel_id)
    video_id=get_videos_ids(channel_id)
    video_data=get_video_data(video_id)
    comment_data=get_comment_data(video_id)
    
    collection=database['channel_data']
    collection.insert_one({'channel_information': channel_data,
                            'video_information': video_data,
                            'comment_information': comment_data,
                            'playlist_information': playlist_data})
    
    return "Data inserted in MongoDB"

#----------------------------------------------------------------------------------------------------------------#

def create_channel_table():
    db=psycopg2.connect(host='localhost',
                       user='postgres',
                       password='27112002Cbm!',
                       database='YouTube_data_harvesting',
                       port='5432')
    cursor=db.cursor()
    
    drop='drop table if exists channels'
    cursor.execute(drop)
    db.commit()

    create='''create table if not exists channels(channel_name VARCHAR(255),
    channel_id VARCHAR(255) PRIMARY KEY,
    channel_subscribers BIGINT,
    total_videos BIGINT,
    channel_views BIGINT,
    channel_description TEXT,
    playlist_id VARCHAR(255))'''

    cursor.execute(create)
    db.commit()

    database=client["YouTube_data_harvesting"]
    collection=database['channel_data']
    channel_list=[]
    for i in collection.find({},{'_id':0,'channel_information':1}):
        channel_list.append(i['channel_information'])
    df=pd.DataFrame(channel_list)

    for i,j in df.iterrows():
        insert='''insert into channels(channel_name,
        channel_id,
        channel_subscribers,
        total_videos,
        channel_views,
        channel_description,
        playlist_id)values(%s,%s,%s,%s,%s,%s,%s)'''

        values=(j['channel_name'],
              j['channel_id'],
              j['subscriber_count'],
              j['total_videos'],
              j['channel_views'],
              j['channel_discription'],
              j['playlist_id'])
        cursor.execute(insert,values)
        db.commit()

def create_playlist_table():
    db=psycopg2.connect(host='localhost',
                       user='postgres',
                       password='27112002Cbm!',
                       database='YouTube_data_harvesting',
                       port='5432')
    cursor=db.cursor()

    drop='drop table if exists playlists'
    cursor.execute(drop)
    db.commit()
    create='''create table if not exists playlists(playlist_name VARCHAR(255),
                playlist_id VARCHAR(255) PRIMARY KEY,
                channel_name VARCHAR(255),
                channel_id VARCHAR(255))'''

    cursor.execute(create)
    db.commit()
    database=client["YouTube_data_harvesting"]
    collection=database['channel_data']
    playlist_list=[]
    for i in collection.find({},{'_id':0,'playlist_information':1}):
            for j in range(len(i['playlist_information'])):
                playlist_list.append(i['playlist_information'][j])
    df=pd.DataFrame(playlist_list)

    for i,j in df.iterrows():
            insert='''insert into playlists(playlist_name,
            channel_id,
            channel_name,
            playlist_id)values(%s,%s,%s,%s)'''

            values=(j['playlist_name'],
                  j['channel_id'],
                  j['channel_name'],
                  j['playlist_id'])

            cursor.execute(insert,values)
            db.commit()

def create_video_table():
    db=psycopg2.connect(host='localhost',
                       user='postgres',
                       password='27112002Cbm!',
                       database='YouTube_data_harvesting',
                       port='5432')
    cursor=db.cursor()

    drop='drop table if exists videos'
    cursor.execute(drop)
    db.commit()

    create='''create table if not exists videos(channel_name VARCHAR(255),
                    video_id VARCHAR(255) PRIMARY KEY,
                    video_name VARCHAR(255),
                    video_description TEXT,
                    tags TEXT,
                    published_at TIMESTAMP,
                    view_count BIGINT,
                    like_count BIGINT,
                    favourite_count INT,
                    comment_count INT,
                    duration INTERVAL,
                    thumbnails VARCHAR(255),
                    caption_status VARCHAR(100))'''

    cursor.execute(create)
    db.commit()

    database=client["YouTube_data_harvesting"]
    collection=database['channel_data']
    videos_list=[]
    for i in collection.find({},{'_id':0,'video_information':1}):
            for j in range(len(i['video_information'])):
                videos_list.append(i['video_information'][j])
    df=pd.DataFrame(videos_list)

    for i,j in df.iterrows():
            insert='''insert into videos(channel_name,
                video_id,
                video_name,
                video_description,
                tags,
                published_at,
                view_count,
                like_count,
                favourite_count,
                comment_count,
                duration,
                thumbnails,
                caption_status)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

            values=(j['channel_name'],
                  j['video_id'],
                  j['video_name'],
                  j['video_description'],
                  j['tags'],
                  j['published_at'],
                  j['view_count'],
                  j['like_count'],
                  j['favourite_count'],
                  j['comment_count'],
                  j['duration'],
                  j['thumbnails'],
                    j['caption_status'])

            cursor.execute(insert,values)
            db.commit()

def create_comment_table():
    db=psycopg2.connect(host='localhost',
                       user='postgres',
                       password='27112002Cbm!',
                       database='YouTube_data_harvesting',
                       port='5432')
    cursor=db.cursor()

    drop='drop table if exists comments'
    cursor.execute(drop)
    db.commit()

    create='''create table if not exists comments(comment_id VARCHAR(255) PRIMARY KEY,
                                                video_id VARCHAR(255),
                                                 comment_text TEXT,
                                                 comment_author VARCHAR(255),
                                                 comment_publishedat TIMESTAMP)'''

    cursor.execute(create)
    db.commit()

    database=client["YouTube_data_harvesting"]
    collection=database['channel_data']
    comments_list=[]
    for i in collection.find({},{'_id':0,'comment_information':1}):
            for j in range(len(i['comment_information'])):
                comments_list.append(i['comment_information'][j])
    df=pd.DataFrame(comments_list)

    for i,j in df.iterrows():
            insert='''insert into comments(comment_id,
                                        video_id,
                                         comment_text,
                                         comment_author,
                                         comment_publishedat)
                                         values(%s,%s,%s,%s,%s)'''

            values=(j['comment_id'],
                    j['video_id'],
                  j['comment_text'],
                  j['comment_author'],
                  j['comment_publishedat'])

            cursor.execute(insert,values)
            db.commit()

def create_tables():
    create_channel_table()
    create_playlist_table()
    create_video_table()
    create_comment_table()

    return "Tables created in SQL"

#--------------------------------------------------------------------------------------------------------------#

def show_channels_table():
        database=client["YouTube_data_harvesting"]
        collection=database['channel_data']
        channel_list=[]
        for i in collection.find({},{'_id':0,'channel_information':1}):
            channel_list.append(i['channel_information'])
        df=sl.dataframe(channel_list)
        return df

def show_playlists_table():
        database=client["YouTube_data_harvesting"]
        collection=database['channel_data']
        playlist_list=[]
        for i in collection.find({},{'_id':0,'playlist_information':1}):
                for j in range(len(i['playlist_information'])):
                    playlist_list.append(i['playlist_information'][j])
        df=sl.dataframe(playlist_list)
        return df

def show_videos_table():
        database=client["YouTube_data_harvesting"]
        collection=database['channel_data']
        videos_list=[]
        for i in collection.find({},{'_id':0,'video_information':1}):
                for j in range(len(i['video_information'])):
                    videos_list.append(i['video_information'][j])
        df=sl.dataframe(videos_list)
        return df

def show_comments_table():
        database=client["YouTube_data_harvesting"]
        collection=database['channel_data']
        comments_list=[]
        for i in collection.find({},{'_id':0,'comment_information':1}):
                for j in range(len(i['comment_information'])):
                    comments_list.append(i['comment_information'][j])
        df=sl.dataframe(comments_list)
        return df


sl.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
sl.markdown("CAPSTONE I (GUVI)")
sl.header("SKILL TAKE AWAY")
sl.caption('Python scripting, Data Collection, MongoDB, API Integration, Data Managment using MongoDB and SQL')
    
channel_id=sl.text_input("Enter the Channel Id:")
if sl.button("Insert data"):
    channel_id_list=[]
    database=client["YouTube_data_harvesting"]
    collection=database['channel_data']
    for i in collection.find({},{'_id':0,'channel_information':1}):
        channel_id_list.append(i['channel_information']['channel_id'])
    
    if channel_id in channel_id_list:
        sl.success("Channel data already exists!")
    else:
        insert=channel_datas(channel_id)
        sl.success(insert)
        display = create_tables()
        sl.success(display)
        sl.success("Insert Done!")

    

show_table = sl.radio("Choose table that has to be displayed",(":green[channels]",
                                                   ":orange[playlists]",
                                                   ":red[videos]",
                                                   ":blue[comments]"), index=None)

if show_table == ":green[channels]":
    show_channels_table()
elif show_table == ":orange[playlists]":
    show_playlists_table()
elif show_table ==":red[videos]":
    show_videos_table()
elif show_table == ":blue[comments]":
    show_comments_table()

db=psycopg2.connect(host='localhost',
                   user='postgres',
                   password='27112002Cbm!',
                   database='YouTube_data_harvesting',
                   port='5432')
cursor=db.cursor()

#-------------------------------------------------------------------------------------------------------------------#

questions='''Questions you can choose:  
     -1. Names of all the videos and their corresponding channels  
     -2. Channels having the most number of videos  
     -3. Top 10 most viewed videos and their respective channels  
     -4. No. of comments made on each video and their corresponding video names  
     -5. Videos having the highest number of likes and their corresponding channel names  
     -6. Total number of likes for each video and their corresponding video names  
     -7. Total number of views for each channel, and their corresponding channel names  
     -8. Names of all the channels that have published videos in the year 2022  
     -9. Average duration of all videos in each channel and their corresponding channel names  
     -10. Videos have the highest number of comments and their corresponding channel names'''

sl.write(questions)

question = sl.selectbox(
    'Select Your Question number',
    ('1','2','3','4','5','6','7','8','9','10'), index=None)

if question == '1':
    query = "select video_name, channel_Name from videos order by published_at desc;"
    cursor.execute(query)
    db.commit()
    tb=cursor.fetchall()
    sl.write(pd.DataFrame(tb, columns=["Video Title","Channel Name"]))

elif question == '2':
    query = "select channel_name,total_videos from channels order by total_videos desc;"
    cursor.execute(query)
    db.commit()
    tb=cursor.fetchall()
    sl.write(pd.DataFrame(tb, columns=["Channel Name","No Of Videos"]))

elif question == '3':
    query = "select channel_name, video_name, view_count from videos where view_count is not null order by view_count desc limit 10;"
    cursor.execute(query)
    db.commit()
    tb = cursor.fetchall()
    sl.write(pd.DataFrame(tb, columns = ["Channel Name","Video Title","Views"]))

elif question == '4':
    query = "select comment_count , video_name from videos where comment_count is not null;"
    cursor.execute(query)
    db.commit()
    tb=cursor.fetchall()
    sl.write(pd.DataFrame(tb, columns=["No. Of Comments", "Video Title"]))

elif question == '5':
    query = "select video_name, channel_name, like_count from videos where like_count is not null order by like_count desc;"
    cursor.execute(query)
    db.commit()
    tb = cursor.fetchall()
    sl.write(pd.DataFrame(tb, columns=["Video Title","Channel Name","Likes"]))

elif question == '6':
    query = "select like_count,video_name from videos;"
    cursor.execute(query)
    db.commit()
    tb = cursor.fetchall()
    sl.write(pd.DataFrame(tb, columns=["Likes","Video Title"]))

elif question == '7':
    query = "select channel_name, channel_views from channels;"
    cursor.execute(query)
    db.commit()
    tb=cursor.fetchall()
    sl.write(pd.DataFrame(tb, columns=["Channel Name","Total Views"]))

elif question == '8':
    query = "select channel_name, video_name, published_at from videos where extract(year from published_at) = 2022;"
    cursor.execute(query)
    db.commit()
    tb=cursor.fetchall()
    sl.write(pd.DataFrame(tb,columns=["Channel Name","Video Name", "Video Publised On"]))

elif question == '9':
    query =  "select channel_name, avg(duration) from videos group by channel_name;"
    cursor.execute(query)
    db.commit()
    tb=cursor.fetchall()
    tb = pd.DataFrame(tb, columns=['Channel Name', 'Average Duration'])
    TB=[]
    for i,j in tb.iterrows():
        channel_title = j['Channel Name']
        average_duration = j['Average Duration']
        average_duration_str = str(average_duration)
        TB.append({"Channel Name": channel_title ,  "Average Duration": average_duration_str})
    sl.write(pd.DataFrame(TB))

elif question == '10':
    query = "select video_name, channel_name, comment_count from videos where comment_count is not null order by comment_count desc;"
    cursor.execute(query)
    db.commit()
    tb=cursor.fetchall()
    sl.write(pd.DataFrame(tb, columns=['Video Name', 'Channel Name', 'N0. Of Comments']))
