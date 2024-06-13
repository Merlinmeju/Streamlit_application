import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import isodate
import plotly.express as px


########################## FUNCTION BLOCK #########################
# 1. Channel
def get_channel_details(api_key, channel_ids):
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.channels().list(part='snippet,contentDetails,statistics,status', id=','.join(channel_ids))
    response = request.execute()
    return response


# 2. Playlist
def get_playlist_items(api_key, playlist_id):
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.playlistItems().list(part='snippet,contentDetails', playlistId=playlist_id, maxResults=10)
    response = request.execute()
    return response


# 3. Video details
def get_video_details(api_key, video_ids):
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.videos().list(part='snippet,contentDetails,statistics', id=','.join(video_ids))
    response = request.execute()
    return response


# 4. Comments
def get_comments(api_key, channel_ids):
    youtube = build('youtube', 'v3', developerKey=api_key)
    comments = []

    for channel_id in channel_ids:
        # Channel title
        channel_request = youtube.channels().list(part='snippet', id=channel_id)
        channel_response = channel_request.execute()
        channel_title = channel_response['items'][0]['snippet']['title']

        # Retrieve - latest videos from each channel
        videos_request = youtube.search().list(part='snippet', channelId=channel_id, type='video', order='date',
                                               maxResults=10)
        videos_response = videos_request.execute()

        for video in videos_response['items']:
            video_id = video['id']['videoId']

            try:
                # Retrieve comments for each video
                comments_request = youtube.commentThreads().list(part='snippet', videoId=video_id, maxResults=10)
                comments_response = comments_request.execute()

                for comment in comments_response['items']:
                    if comment['snippet']['topLevelComment']['snippet']['videoId'] == video_id:
                        comment_data = {
                            'channel_title': channel_title,
                            'channel_id': channel_id,
                            'comment_id': comment['id'],
                            'video_id': video_id,
                            'comment_text': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                            'comment_author': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            'comment_published_date': comment['snippet']['topLevelComment']['snippet']['publishedAt']
                        }
                        comments.append(comment_data)
            except HttpError as e:
                if e.resp.status == 403:
                    # Skip videos with disabled comments
                    st.warning(f"Comments are disabled for video: {video_id}")
                else:
                    raise

    return comments


######################### STORAGE OF FETCHED DATA IN MYSQL ##################################
def store_data_in_mysql(channels, playlists, videos, comments):
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='youtube_data',
            user='root',
            password='root'
        )

        if connection.is_connected():
            cursor = connection.cursor()

            ############### CREATION OF TABLES ###########
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS channel (
                    channel_id VARCHAR(255) PRIMARY KEY,
                    channel_name VARCHAR(255),
                    channel_type VARCHAR(255),
                    channel_views BIGINT,
                    channel_description TEXT,
                    channel_status VARCHAR(255)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS playlist (
                    playlist_id VARCHAR(255) PRIMARY KEY,
                    channel_id VARCHAR(255),
                    playlist_name VARCHAR(255),
                    FOREIGN KEY (channel_id) REFERENCES channel(channel_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS video (
                    video_id VARCHAR(255) PRIMARY KEY,
                    channel_id VARCHAR(255),
                    playlist_id VARCHAR(255),
                    video_name VARCHAR(255),
                    video_description TEXT,
                    published_date DATETIME,
                    view_count BIGINT,
                    like_count BIGINT,
                    dislike_count BIGINT,
                    favorite_count BIGINT,
                    comment_count BIGINT,
                    duration INT,
                    thumbnail VARCHAR(255),
                    caption_status VARCHAR(255),
                    FOREIGN KEY (channel_id) REFERENCES channel(channel_id),
                    FOREIGN KEY (playlist_id) REFERENCES playlist(playlist_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Comment (
                    channel_id VARCHAR(255),
                    comment_id VARCHAR(255) PRIMARY KEY,
                    video_id VARCHAR(255),
                    comment_text TEXT,
                    comment_author VARCHAR(255),
                    comment_published_date DATETIME,
                    FOREIGN KEY (channel_id) REFERENCES channel(channel_id)

                )
            ''')

            ############## DATA INSERTION INTO TABLES ######################
            for channel in channels:
                cursor.execute('''
                    INSERT INTO channel (channel_id, channel_name, channel_type, channel_views, channel_description, channel_status)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        channel_name = VALUES(channel_name),
                        channel_type = VALUES(channel_type),
                        channel_views = VALUES(channel_views),
                        channel_description = VALUES(channel_description),
                        channel_status = VALUES(channel_status)
                ''', (channel['channel_id'], channel['channel_name'], channel['channel_type'], channel['channel_views'],
                      channel['channel_description'], channel['channel_status']))

            for playlist in playlists:
                cursor.execute('''
                    INSERT INTO playlist (playlist_id, channel_id, playlist_name)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        playlist_name = VALUES(playlist_name)
                ''', (playlist['playlist_id'], playlist['channel_id'], playlist['playlist_name']))

            for video in videos:
                published_date = datetime.strptime(video['published_date'], "%Y-%m-%dT%H:%M:%SZ").strftime(
                    "%Y-%m-%d %H:%M:%S")
                view_count = int(video['view_count']) if video['view_count'] != 'N/A' else 0
                like_count = int(video['like_count']) if video['like_count'] != 'N/A' else 0
                dislike_count = int(video['dislike_count']) if video['dislike_count'] != 'N/A' else 0
                favorite_count = int(video['favorite_count']) if video['favorite_count'] != 'N/A' else 0
                comment_count = int(video['comment_count']) if video['comment_count'] != 'N/A' else 0

                cursor.execute('''
                    INSERT INTO video (video_id, channel_id, playlist_id, video_name, video_description, published_date, view_count, like_count, dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        video_name = VALUES(video_name),
                        video_description = VALUES(video_description),
                        published_date = VALUES(published_date),
                        view_count = VALUES(view_count),
                        like_count = VALUES(like_count),
                        dislike_count = VALUES(dislike_count),
                        favorite_count = VALUES(favorite_count),
                        comment_count = VALUES(comment_count),
                        duration = VALUES(duration),
                        thumbnail = VALUES(thumbnail),
                        caption_status = VALUES(caption_status)
                ''', (video['video_id'], video['channel_id'], video['playlist_id'], video['video_name'],
                      video['video_description'], published_date, view_count, like_count, dislike_count, favorite_count,
                      comment_count, video['duration'], video['thumbnail'], video['caption_status']))

            # Data Insertion
            for comment in comments:
                # Check if the video ID exists in the video table
                cursor.execute('SELECT COUNT(*) FROM video WHERE video_id = %s', (comment['video_id'],))
                video_exists = cursor.fetchone()[0]

                if video_exists:
                    comment_published_date = datetime.strptime(comment['comment_published_date'],
                                                               "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
                    # Insert comment data
                    cursor.execute('''
                        INSERT INTO Comment (channel_id, comment_id, video_id, comment_text, comment_author, comment_published_date)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            comment_text = VALUES(comment_text),
                            comment_author = VALUES(comment_author),
                            comment_published_date = VALUES(comment_published_date)
                    ''', (
                        comment['channel_id'], comment['comment_id'], comment['video_id'], comment['comment_text'],
                        comment['comment_author'],
                        comment_published_date
                    ))
                else:
                    print(
                        f"Video with ID {comment['video_id']} does not exist in the video table. Skipping comment insertion.")


            connection.commit()
            st.success("Data stored in MySQL database successfully!")


    except Error as e:
        st.error(f"Error connecting to MySQL: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


##################### AUTOMATIC QUERY RUN TO DISPLAY RESULTS IN STREAMLIT #############################
def run_queries_and_display():
    connection = None
    try:
        connection = mysql.connector.connect(
            host='localhost',  # Change to your host
            database='youtube_data',  # Change to your database name
            user='root',  # Change to your MySQL user
            password='root'  # Change to your MySQL password
        )

        if connection.is_connected():
            cursor = connection.cursor()

            queries = {
                "Video Names and Corresponding Channels": '''
                    SELECT video_name, channel_name
                    FROM video
                    JOIN channel ON video.channel_id = channel.channel_id
                ''',
                "Channels with Most Videos": '''
                    SELECT channel_name, COUNT(*) AS video_count
                    FROM video
                    JOIN channel ON video.channel_id = channel.channel_id
                    GROUP BY channel_name
                    ORDER BY video_count DESC
                ''',
                "Top 10 Most Viewed Videos": '''
                    SELECT video_name, view_count
                    FROM video
                    ORDER BY view_count DESC
                    LIMIT 10
                ''',
                "Comments on Each Video": '''
                    SELECT video_name, COUNT(*) AS comment_count
                    FROM video
                    JOIN Comment ON video.video_id = Comment.video_id
                    GROUP BY video_name
                    ORDER BY comment_count DESC
                ''',
                "Videos with Highest Likes": '''
                    SELECT video_name, like_count
                    FROM video
                    ORDER BY like_count DESC
                    LIMIT 10
                ''',
                "Total Likes and Dislikes for Each Video": '''
                    SELECT video_name, like_count, dislike_count
                    FROM video
                    ORDER BY (like_count + dislike_count) DESC
                ''',
                "Total Views for Each Channel": '''
                    SELECT channel_name, SUM(view_count) AS total_views
                    FROM video
                    JOIN channel ON video.channel_id = channel.channel_id
                    GROUP BY channel_name
                    ORDER BY total_views DESC
                ''',
                "Channels with Videos Published in 2024": '''
                    SELECT channel_name, COUNT(*) AS video_count
                    FROM video
                    JOIN channel ON video.channel_id = channel.channel_id
                    WHERE YEAR(published_date) = 2024
                    GROUP BY channel_name
                ''',
                "Average Duration of Videos in Each Channel": '''
                    SELECT channel_name, AVG(duration) AS avg_duration
                    FROM video
                    JOIN channel ON video.channel_id = channel.channel_id
                    GROUP BY channel_name
                    ORDER BY avg_duration DESC
                ''',
                "Videos with Highest Number of Comments": '''
                    SELECT video_name, comment_count
                    FROM video
                    ORDER BY comment_count DESC
                    LIMIT 10
                '''
            }

            st.header("Query Results")

            for query_name, query in queries.items():
                if st.button(query_name):
                    cursor.execute(query)
                    result = cursor.fetchall()
                    df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
                    st.dataframe(df)

                    # Generate graphs using Plotly
                    if query_name == "Video Names and Corresponding Channels":
                        fig = px.bar(df, x='video_name', y='channel_name',
                                     title="Video Names and Corresponding Channels")
                    elif query_name == "Channels with Most Videos":
                        fig = px.bar(df, x='channel_name', y='video_count', title="Channels with Most Videos")
                    elif query_name == "Top 10 Most Viewed Videos":
                        fig = px.bar(df, x='video_name', y='view_count', title="Top 10 Most Viewed Videos")
                    elif query_name == "Comments on Each Video":
                        fig = px.bar(df, x='video_name', y='comment_count', title="Comments on Each Video")
                    elif query_name == "Videos with Highest Likes":
                        fig = px.bar(df, x='video_name', y='like_count', title="Videos with Highest Likes")
                    elif query_name == "Total Likes and Dislikes for Each Video":
                        fig = px.bar(df, x='video_name', y=['like_count', 'dislike_count'],
                                     title="Total Likes and Dislikes for Each Video", barmode='group')
                    elif query_name == "Total Views for Each Channel":
                        fig = px.bar(df, x='channel_name', y='total_views', title="Total Views for Each Channel")
                    elif query_name == "Channels with Videos Published in 2024":
                        fig = px.bar(df, x='channel_name', y='video_count',
                                     title="Channels with Videos Published in 2024")
                    elif query_name == "Average Duration of Videos in Each Channel":
                        fig = px.bar(df, x='channel_name', y='avg_duration',
                                     title="Average Duration of Videos in Each Channel")
                    elif query_name == "Videos with Highest Number of Comments":
                        fig = px.bar(df, x='video_name', y='comment_count',
                                     title="Videos with Highest Number of Comments")

                    st.plotly_chart(fig)

    except Error as e:
        st.error(f"Error while connecting to MySQL: {e}")

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

############### MAIN FUNCTION ######################


st.title("YouTube Channel Data Analysis Application")


def main():
    # Sidebar navigation
    st.sidebar.title('Navigation')
    app_mode = st.sidebar.selectbox("Choose the app mode", ["Home", "Fetch and Store", "Analysis"])

    if app_mode == 'Home':
        st.header("Welcome to the YouTube Channel Data Analysis App!")
        st.write("Select an option from the sidebar to get started.")

    elif app_mode == 'Fetch and Store':
        st.header("Fetch and Store Data")
        api_key = st.text_input("Enter YouTube Data API v3 Key", type="password")
        channel_ids_input = st.text_input("Enter YouTube Channel IDs (comma-separated)")

        if st.button("Fetch and Store Data"):
            if api_key and channel_ids_input:
                channel_ids = [channel_id.strip() for channel_id in channel_ids_input.split(',')]
                channels = []
                playlists = []
                videos = []
                comments = []

                channel_details = get_channel_details(api_key, channel_ids)
                for channel in channel_details['items']:
                    channel_id = channel['id']
                    channel_name = channel['snippet']['title']
                    channel_type = channel['snippet'].get('type', 'N/A')
                    channel_views = int(channel['statistics'].get('viewCount', 0))
                    channel_description = channel['snippet'].get('description', 'N/A')
                    channel_status = channel['status'].get('privacyStatus', 'N/A')

                    channels.append({
                        'channel_id': channel_id,
                        'channel_name': channel_name,
                        'channel_type': channel_type,
                        'channel_views': channel_views,
                        'channel_description': channel_description,
                        'channel_status': channel_status
                    })

                    playlist_id = channel['contentDetails']['relatedPlaylists']['uploads']
                    playlist_items = get_playlist_items(api_key, playlist_id)
                    for item in playlist_items['items']:
                        video_id = item['contentDetails']['videoId']
                        video_details = get_video_details(api_key, [video_id])

                        for video in video_details['items']:
                            video_id = video['id']
                            video_name = video['snippet']['title']
                            video_description = video['snippet'].get('description', 'N/A')
                            published_date = video['snippet']['publishedAt']
                            view_count = video['statistics'].get('viewCount', 'N/A')
                            like_count = video['statistics'].get('likeCount', 'N/A')
                            dislike_count = video['statistics'].get('dislikeCount', 'N/A')
                            favorite_count = video['statistics'].get('favoriteCount', 'N/A')
                            comment_count = video['statistics'].get('commentCount', 'N/A')
                            duration = isodate.parse_duration(video['contentDetails']['duration']).total_seconds()
                            thumbnail = video['snippet']['thumbnails']['default']['url']
                            caption_status = video['contentDetails'].get('caption', 'N/A')
                            #comments

                            videos.append({
                                'video_id': video_id,
                                'channel_id': channel_id,
                                'playlist_id': playlist_id,
                                'video_name': video_name,
                                'video_description': video_description,
                                'published_date': published_date,
                                'view_count': view_count,
                                'like_count': like_count,
                                'dislike_count': dislike_count,
                                'favorite_count': favorite_count,
                                'comment_count': comment_count,
                                'duration': duration,
                                'thumbnail': thumbnail,
                                'caption_status': caption_status
                            })

                        playlists.append({
                            'playlist_id': playlist_id,
                            'channel_id': channel_id,
                            'playlist_name': 'Uploads'
                        })

                comments = get_comments(api_key, channel_ids)
                store_data_in_mysql(channels, playlists, videos, comments)
            else:
                st.error("Please enter both YouTube Data API key and channel IDs.")

    elif app_mode == 'Analysis':
        st.header("Data Analysis")

        # Run queries and display results
        run_queries_and_display()


if __name__ == "__main__":
    main()