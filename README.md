**Streamlit YouTube Channel Data Analysis Application**
This application is designed to fetch data from YouTube channels using the YouTube Data API v3, store it in a MySQL database, and provide analysis and visualization of the data using Streamlit and Plotly.

**Table of Contents**
* Features
* Requirements
* Installation
* Usage
* Functions
* Data Storage
* Query and Analysis
* API Key and Channel IDs
* Features
* Fetch channel details, playlist items, video details, and comments from YouTube.
* Store the fetched data in a MySQL database.
* Display data and visualizations in an interactive Streamlit web application.
* Run predefined queries and display results with interactive charts.
* Requirements
* Python 3.7 or higher
* Streamlit
* Pandas
* Google API Client
* MySQL Connector
* Plotly
* Installation_
**Clone this repository:**
git clone https://github.com/yourusername/youtube-data-analysis.git
cd youtube-data-analysis

**Install the required packages:**
pip install streamlit pandas google-api-python-client mysql-connector-python plotly
Ensure MySQL is installed and running on your local machine or server.

**Usage**
Run the Streamlit application:

streamlit run app.py
Open your browser and go to http://localhost:8501.

Use the sidebar to navigate between "Home", "Fetch and Store", and "Analysis" pages.

**Functions**
1. Fetching Data
2. get_channel_details(api_key, channel_ids): Retrieves details of specified YouTube channels.
3. get_playlist_items(api_key, playlist_id): Retrieves items of a specified playlist.
4. get_video_details(api_key, video_ids): Retrieves details of specified videos.
5. get_comments(api_key, channel_ids): Retrieves comments from the latest videos of specified channels.
6. Storing Data
7. store_data_in_mysql(channels, playlists, videos, comments): Stores fetched data in a MySQL database with predefined schema and foreign key relationships.
8. Data Storage
9. The MySQL database schema includes the following tables:
10. channel: Stores channel details.
11. playlist: Stores playlist details.
12. video: Stores video details.
13. comment: Stores comment details.
14. The tables have appropriate foreign key relationships to ensure data integrity.

**Query and Analysis**
The application includes several predefined queries to analyze the data:

1. Video Names and Corresponding Channels
2. Channels with Most Videos
3. Top 10 Most Viewed Videos
4. Comments on Each Video
5. Videos with Highest Likes
6. Total Likes and Dislikes for Each Video
7. Total Views for Each Channel
8. Channels with Videos Published in 2024
9. Average Duration of Videos in Each Channel
10. Videos with Highest Number of Comments
The results are displayed in the Streamlit app with interactive charts created using Plotly.

**API Key and Channel IDs**
API Key: AIzaSyAK9h3eecxjWtzbaiVPluuGww-CXvL8OXA
Channel IDs:
UCN_QTTqTo4RKaa92VxM298g
UCB_9w_j7jqEOqfhKejM7b-g
UCfm4y4rHF5HGrSr-qbvOwOg
UCQmxcMxjYcBM5Pel4qUW2hA
UCDYetMc6gOLkhIiNzFyrJPA
UCDWnjq_nvuqyqeKKK_avLKA
