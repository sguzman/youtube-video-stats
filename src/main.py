import bs4
import requests
import queue
import threading
import psycopg2
import json
import os
from multiprocessing.dummy import Pool
import random
import sys
import traceback


seen = queue.Queue()
cores = 8
pool = Pool(cores)
limit = 15865
table = 'chans'


def get_incumbent_chans():
    postgresql_select_query = f'SELECT chan_serial FROM youtube.channels.{table}'
    cursor = conn.cursor()
    cursor.execute(postgresql_select_query)
    records = cursor.fetchall()

    ignore = set()
    for i in records:
        ignore.add(i[0])

    print(len(ignore), 'channels from table')

    cursor.close()
    return ignore


conn = psycopg2.connect(user='root', password='', host='127.0.0.1', port='5432', database='youtube')
table_chans = get_incumbent_chans()


def insert_vids(cursor, data):
    sql_insert_chann = f'INSERT INTO youtube.channels.{table} ' \
                       '(chan_serial, title, custom_url, description, joined, thumbnail, topic_ids, ' \
                       'topic_categories, privacy_status, is_linked, long_uploads, tracking_id, ' \
                       'moderate_comments, show_related_channels, show_browse, banner_image, subs) ' \
                       'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

    for datum in data:
        print(datum[0])
        cursor.execute(sql_insert_chann, datum)


def seen_daemon():
    while True:
        idx, channels = seen.get(block=True)
        cursor = conn.cursor()
        insert_vids(cursor, channels)
        conn.commit()
        cursor.close()


def page(idx):
    url = f'https://dbase.tube/chart/channels/subscribers/all?page={idx}&spf=navigate'
    return requests.get(url)


def soup_page(idx):
    req = page(idx)
    text = req.text
    json_data = json.loads(text)
    body = json_data['body']['spf_content']

    return bs4.BeautifulSoup(body, 'html.parser')


def nest_index(obj, indexes):
    tmp = obj
    for idx in indexes:
        if idx in tmp:
            tmp = tmp[idx]
        else:
            return None

    return tmp


def get_channel_info(channel_id):
    url = f'https://www.googleapis.com/youtube/v3/channels'

    params = {
        'part': 'snippet,contentDetails,brandingSettings,contentOwnerDetails,invideoPromotion,localizations,status,topicDetails,statistics',
        'id': channel_id,
        'key': os.environ['API_KEY']
    }

    text = requests.get(url, params=params).text
    json_body = json.loads(text)

    if 'items' not in json_body:
        return None

    if len(json_body['items']) == 0:
        return None

    items = json_body['items'][0]
    snippet = items['snippet']
    desc = nest_index(snippet, ['description'])
    if desc is not None:
        if len(desc) == 0:
            desc = None
    else:
        desc = desc.replace('\0', ' ')

    data = [channel_id,
            nest_index(snippet, ['title']),
            nest_index(snippet, ['customUrl']),
            desc,
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
            items['statistics']['subscriberCount'],
            ]

    return data


def vids(i):
    try:
        pg = soup_page(i)
        channels = []
        select = pg.select('a.aj.row')
        for a_href in select:
            path = a_href['href'].split('/')
            channel_id = path[2]

            if channel_id not in table_chans:
                info = get_channel_info(channel_id)
                if info is not None:
                    channels.append(info)

        seen.put((i, channels))
    except Exception as e:
        print(e, file=sys.stderr)
        traceback.print_exc()


def main():
    threading.Thread(target=seen_daemon, daemon=True).start()

    nums = list(range(limit))
    random.shuffle(nums)

    pool.map(vids, nums)
    print('done')


main()
