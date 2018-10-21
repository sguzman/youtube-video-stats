import requests
import random
import psycopg2
import json
import os
import datetime


db_user = 'postgres'
db_pass = ''
db_host = 'postgres-service.youtube.svc.cluster.local'
db_db = 'youtube'
db_port = '5432'

keys = os.environ['API_KEY'].split('|')


def connection():
    conn = psycopg2.connect(user=db_user, password=db_pass, host=db_host, port=db_port, database=db_db)
    return conn


def get_key():
    return random.choice(keys)


def insert_vids(conn, data):
    sql_insert_vids = 'INSERT INTO youtube.entities.vids ' \
                       '(serial, published_at, channel_id, title, description, thumbnail, category_id, ' \
                      'live_broadcasting_content, default_audio_language, duration, dimension, definition, caption, ' \
                      'licensed_content, projection, upload_status, privacy_status, license, embeddable, ' \
                      'public_stats_viewable, relevant_topic_ids, topic_categories, view_count, like_count, ' \
                      'dislike_count, favorite_count, comment_count) ' \
                       'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ' \
                      '%s, %s, %s, %s, %s, %s, %s) ' \
                       'ON CONFLICT DO NOTHING'

    cursor = conn.cursor()
    print(data)
    cursor.execute(sql_insert_vids, data)
    conn.commit()
    cursor.close()


def nest_index(obj, indexes):
    tmp = obj
    for idx in indexes:
        if idx in tmp:
            tmp = tmp[idx]
        else:
            return None

    return tmp


def interval(string):
    raw_str = string[2:]
    days = 0
    hours = 0
    minutes = 0
    seconds = 0

    days_idx = raw_str.find('D')
    if days_idx != -1:
        days = int(raw_str[:days_idx])
        raw_str = raw_str[days_idx + 1:]

    hours_idx = raw_str.find('H')
    if hours_idx != -1:
        hours = int(raw_str[:hours_idx])
        raw_str = raw_str[hours_idx + 1:]

    mins_idx = raw_str.find('M')
    if mins_idx != -1:
        minutes = int(raw_str[:mins_idx])
        raw_str = raw_str[mins_idx + 1:]

    secs_idx = raw_str.find('S')
    if secs_idx != -1:
        seconds = int(raw_str[:secs_idx])

    return datetime.timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


def live_broad(string):
    if string == 'none':
        return None
    return string


def none_to_num(raw_str):
    if raw_str is None:
        return 0
    return int(raw_str)


def get_data(i):
    snippet = i['snippet']
    data = [i['id'],
            nest_index(snippet, ['publishedAt']),
            nest_index(snippet, ['channelId']),
            nest_index(snippet, ['title']),
            nest_index(snippet, ['description']),
            nest_index(snippet, ['thumbnails', 'default', 'url']),
            int(nest_index(snippet, ['categoryId'])),
            live_broad(nest_index(snippet, ['liveBroadcastContent'])),
            nest_index(snippet, ['defaultAudioLanguage']),
            interval(nest_index(i, ['contentDetails', 'duration'])),
            nest_index(i, ['contentDetails', 'dimension']),
            nest_index(i, ['contentDetails', 'definition']),
            'true' == nest_index(i, ['contentDetails', 'caption']),
            nest_index(i, ['contentDetails', 'licensedContent']),
            nest_index(i, ['contentDetails', 'projection']),
            nest_index(i, ['status', 'uploadStatus']),
            nest_index(i, ['status', 'privacyStatus']),
            nest_index(i, ['status', 'license']),
            nest_index(i, ['status', 'embeddable']),
            nest_index(i, ['status', 'publicStatsViewable']),
            nest_index(i, ['topicDetails', 'relevantTopicIds∂']),
            nest_index(i, ['topicDetails', 'topicCategories']),
            none_to_num(nest_index(i, ['statistics', 'viewCount'])),
            none_to_num(nest_index(i, ['statistics', 'likeCount'])),
            none_to_num(nest_index(i, ['statistics', 'dislikeCount'])),
            none_to_num(nest_index(i, ['statistics', 'favoriteCount'])),
            none_to_num(nest_index(i, ['statistics', 'commentCount']))
            ]

    return data


def get_video_info(videos):
    url = 'https://www.googleapis.com/youtube/v3/videos'
    vids = [a[1] for a in videos]

    params = {
        'part': 'snippet,contentDetails,liveStreamingDetails,recordingDetails,status,topicDetails,statistics',
        'id': ','.join(vids),
        'key': get_key()
    }

    text = requests.get(url, params=params).text
    json_body = json.loads(text)
    items = json_body['items']

    datas = []
    for i in items:
        data = get_data(i)
        datas.append(data)

    return datas


def get_videos():
    conn = connection()
    sql = 'SELECT id, serial FROM youtube.entities.videos ORDER BY RANDOM() LIMIT 50'
    cursor = conn.cursor()
    cursor.execute(sql)
    records = cursor.fetchall()

    cursor.close()
    conn.close()
    return records


def main():
    while True:
        sample = get_videos()
        vids = get_video_info(sample)
        for v in vids:
            conn = connection()
            insert_vids(conn, v)
            conn.close()


if __name__ == '__main__':
    main()
