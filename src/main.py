import requests
import random
import psycopg2
import json
import os


db_user = 'postgres'
db_pass = ''
db_host = '192.168.1.63'
db_db = 'youtube'
db_port = '30000'

keys = os.environ['API_KEY'].split('|')


def connection():
    conn = psycopg2.connect(user=db_user, password=db_pass, host=db_host, port=db_port, database=db_db)
    return conn


def get_key():
    return random.choice(keys)


def insert_vids(conn, data):
    sql_insert_chann = 'INSERT INTO youtube.entities.chans ' \
                       '(id, serial, title, custom_url, description, joined, thumbnail, topic_ids, ' \
                       'topic_categories, privacy_status, is_linked, long_uploads, tracking_id, ' \
                       'moderate_comments, show_related_channels, show_browse, banner_image, subs,' \
                       'video_count, video_views) ' \
                       'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ' \
                       'ON CONFLICT DO NOTHING'

    cursor = conn.cursor()
    print(data)
    cursor.execute(sql_insert_chann, data)
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


def get_data(i, ids_by_chans):
    snippet = i['snippet']
    data = [ids_by_chans[i['id']],
            i['id'],
            nest_index(snippet, ['title']),
            nest_index(snippet, ['customUrl']),
            nest_index(snippet, ['description']),
            nest_index(snippet, ['publishedAt']),
            nest_index(snippet, ['thumbnails', 'url']),
            nest_index(i, ['topicDetails', 'topicIds']),
            nest_index(i, ['topicDetails', 'topicCategories']),
            nest_index(i, ['status', 'privacyStatus']),
            nest_index(i, ['status', 'isLinked']),
            nest_index(i, ['status', 'longUploadsStatus']),
            nest_index(i, ['brandingSettings', 'channel', 'trackingAnalyticsAccountId']),
            nest_index(i, ['brandingSettings', 'channel', 'moderateComments']),
            nest_index(i, ['brandingSettings', 'channel', 'showRelatedChannels']),
            nest_index(i, ['brandingSettings', 'channel', 'showBrowseView']),
            nest_index(i, ['brandingSettings', 'image', 'bannerImageUrl']),
            i['statistics']['subscriberCount'],
            i['statistics']['videoCount'],
            i['statistics']['viewCount']
            ]

    return data


def get_channel_info(channels):
    url = f'https://www.googleapis.com/youtube/v3/channels'
    chans = [a[1] for a in channels]
    ids_by_chans = {a[1]: a[0] for a in channels}

    params = {
        'part': 'snippet,contentDetails,brandingSettings,contentOwnerDetails,invideoPromotion,localizations,status,topicDetails,statistics',
        'id': ','.join(chans),
        'key': get_key()
    }

    text = requests.get(url, params=params).text
    json_body = json.loads(text)

    items = json_body['items']

    datas = []
    for i in items:
        data = get_data(i, ids_by_chans)
        datas.append(data)

    return datas


def get_channels():
    conn = connection()
    sql = 'SELECT id, serial FROM youtube.entities.channels ORDER BY RANDOM() LIMIT 50'
    cursor = conn.cursor()
    cursor.execute(sql)
    records = cursor.fetchall()

    cursor.close()
    conn.close()
    return records


def main():
    while True:
        sample = get_channels()
        chans = get_channel_info(sample)
        for c in chans:
            conn = connection()
            insert_vids(conn, c)
            conn.close()


if __name__ == '__main__':
    main()
