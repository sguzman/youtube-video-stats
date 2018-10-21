import requests
import random
import psycopg2
import json
import os


db_user = 'root'
db_pass = ''
db_host = 'localhost'
db_db = 'youtube'
db_port = '5432'

keys = os.environ['API_KEY'].split('|')


def connection():
    conn = psycopg2.connect(user=db_user, password=db_pass, host=db_host, port=db_port, database=db_db)
    return conn


def get_key():
    return random.choice(keys)


def insert_vids(data):
    conn = connection()

    sql_insert_chann = f'INSERT INTO youtube.channels.chans ' \
                       '(chan_serial, title, custom_url, description, joined, thumbnail, topic_ids, ' \
                       'topic_categories, privacy_status, is_linked, long_uploads, tracking_id, ' \
                       'moderate_comments, show_related_channels, show_browse, banner_image, subs) ' \
                       'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

    cursor = conn.cursor()
    for datum in data:
        print(datum[0])
        cursor.execute(sql_insert_chann, datum)
    conn.commit()
    cursor.close()
    conn.close()


def nest_index(obj, indexes):
    tmp = obj
    for idx in indexes:
        if idx in tmp:
            tmp = tmp[idx]
        else:
            return None

    return tmp


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
        snippet = i['snippet']
        data = [ids_by_chans[i['id']],
                             i['id'],
                nest_index(snippet, ['title']),
                nest_index(snippet, ['customUrl']),
                nest_index(snippet, ['description']),
                nest_index(snippet, ['publishedAt']),
                nest_index(snippet, ['thumbnails', 'url']),
                nest_index(items, ['topicDetails', 'topicIds']),
                nest_index(items, ['topicDetails', 'topicCategories']),
                nest_index(items, ['status', 'privacyStatus']),
                nest_index(items, ['status', 'isLinked']),
                nest_index(items, ['status', 'longUploadsStatus']),
                nest_index(items, ['brandingSettings', 'channel', 'trackingAnalyticsAccountId']),
                nest_index(items, ['brandingSettings', 'channel', 'moderateComments']),
                nest_index(items, ['brandingSettings', 'channel', 'showRelatedChannels']),
                nest_index(items, ['brandingSettings', 'channel', 'showBrowseView']),
                nest_index(items, ['brandingSettings', 'image', 'bannerImageUrl']),
                i['statistics']['subscriberCount'],
                i['statistics']['videoCount'],
                i['statistics']['viewCount']
                ]

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
        get_channel_info(sample)


if __name__ == '__main__':
    main()