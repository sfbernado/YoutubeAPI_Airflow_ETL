import os
import json
import pandas as pd
import googleapiclient.discovery
import googleapiclient.errors

# -*- coding: utf-8 -*-

def run_youtube_etl():
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    api_key = "AIzaSyC_-LZwMvYSSRjpCQX3-qumbNh88nKq72M"
    channel_id = "UC1dI4tO13ApuSX0QeX8pHng" # channel id of GadgetIn
    playlist_id = ""

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = api_key
    )

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute() 

    for item in response['items']:
        playlist_id = item['contentDetails']['relatedPlaylists']['uploads']

    videoId_list = []

    request = youtube.playlistItems().list(
        part="contentDetails",
        playlistId=playlist_id
    )
    response = request.execute()

    for item in response['items']:
        videoId_list.append(item['contentDetails']['videoId'])

    while response.get('nextPageToken', None):
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            pageToken=response['nextPageToken']
        )
        response = request.execute()

        for item in response['items']:
            videoId_list.append(item['contentDetails']['videoId'])

    data = []

    for videoId in videoId_list: 
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=videoId
        )
        response = request.execute()

        for item in response['items']:
            data.append({
                'videoId': item['id'],
                'title': item['snippet']['title'],
                'viewCount': item['statistics']['viewCount'],
                'likeCount': item['statistics']['likeCount'],
                'commentCount': item['statistics']['commentCount'],
                'publishedAt': item['snippet']['publishedAt']
            })
        
        while response.get('nextPageToken', None):
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=videoId,
                pageToken=response['nextPageToken']
            )
            response = request.execute()

            for item in response['items']:
                data.append({
                    'videoId': item['id'],
                    'title': item['snippet']['title'],
                    'viewCount': item['statistics']['viewCount'],
                    'likeCount': item['statistics']['likeCount'],
                    'commentCount': item['statistics']['commentCount'],
                    'publishedAt': item['snippet']['publishedAt']
                })

    print(f'Finished processing {len(data)} video(s).')

    with open('responseChannelVideos.json', 'w', encoding='utf-8') as f:
        json.dump(response, f, ensure_ascii=False, indent=4)
    
    df = pd.DataFrame(data)
    df.to_csv('s3://stani-airflow-bucket/channelVideos_data.csv', index=False)
