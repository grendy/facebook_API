import urllib2
import json
import time
from kafka import KafkaProducer, KafkaConsumer
import scrapy
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.keys import Keys
from pyvirtualdisplay import Display
from selenium.webdriver.common.proxy import *
from scrapy.http import TextResponse
import MySQLdb
import re
import config
import datetime
from ssdb import SSDB
class Gtoken():
    count = 0
    ssdb = SSDB(host='localhost', port=8888)
    while True:
        post_id = ssdb.get('raw'+str(count))
        dapet_komen(post_id)
        count +=1

def dapet_komen(post_id):
    global link, itungan, hitung

    print "======================================="
    print "AMBIL COMMENT"
    print "======================================="
    if itungan == 0:
        link = "https://graph.facebook.com/v2.8/" + post_id + "/comments?access_token=" + token
    else:
        link = link
    for loop in range(0, 20):
        try:
            requests_comment = urllib2.urlopen(link)
            comment = json.loads(requests_comment.read())
            requests_comment.close()
            break
        except:
            try:
                requests_comment.close()
            except:
                pass
    try:
        komen = comment
    except:
        Gtoken().start_requests()
    if "data" in comment:
        list_comment = comment['data']
        for comment in list_comment:
            itungan += 1
            comment_id = comment['from']['id']
            com_id = comment['id']
            try:
                comment_nama = str(comment['from']['name'])
            except:
                comment_nama = comment['from']['name'].encode('utf-8')
            try:
                comment_pesan = str(comment['message'])
            except:
                comment_pesan = comment['message'].encode('utf-8')
            comment_tanggal = comment['created_time']
            url4 = "https://graph.facebook.com/v2.8/" + comment_id + "?fields=picture&access_token=" + token
            for loop in range(0, 20):
                try:
                    request_pp = urllib2.urlopen(url4)
                    url_pp = json.loads(request_pp.read())
                    request_pp.close()
                    break
                except:
                    try:
                        request_pp.close()
                    except:
                        pass
            desc = ""
            caption = ""
            bersih = comment_tanggal.split('T')[0]
            bersih = bersih.split('-')
            d_year = bersih[0]
            d_month = bersih[0] + bersih[1]
            d_day = bersih[0] + bersih[1] + bersih[2]
            json_comment = json.dumps(
                {'type': 'comment', 'fb_status_id': posting_id, 'comment_from_id': comment_id, 'from': comment_nama,
                 'id': com_id, 'fb_id': akun_id, 'like_from_id': kosong, "crawler_id": id_crawler,
                 'message': comment_pesan, 'created_at': comment_tanggal, 'desc': desc, 'is_comment': 1,
                 'caption': caption, 'd_year': d_year, 'd_month': d_month, 'd_day': d_day})
            for timpa in range(len(config.ray)):
                json_comment = json_comment.replace(config.ray[timpa], "")
            # json_comment = json.dumps({'type' : 'comment', 'id' : comment_id, 'nama' : comment_nama, 'post_id' : post_id, 'message' : comment_pesan, 'tanggal_dibuat' : comment_tanggal})
            for kafka in range(0, 20):
                try:
                    prod = KafkaProducer(bootstrap_servers=config.bootstrap_servers)
                    prod.send(config.kafka_topic, json_comment)
                    print "======================================="
                    print json_comment
                    print "SELESAI KIRIM KE FB_API"
                    print "KOMEN ke - " + str(itungan)
                    print "======================================="
                    break
                except:
                    pass
        if "paging" in komen:
            if "next" in komen['paging']:
                link = komen['paging']['next']
                dapet_komen(post_id)
    hitung = 0


def dapet_suka(post_id):
    global situs, hitung
    print "======================================="
    print "AMBIL LIKE"
    print "======================================="
    if hitung == 0:
        situs = "https://graph.facebook.com/v2.8/" + post_id + "/reactions?access_token=" + token
    else:
        situs = situs
    for loop in range(0, 20):
        try:
            requests_suka = urllib2.urlopen(situs)
            suka = json.loads(requests_suka.read())
            requests_suka.close()
            break
        except:
            try:
                requests_suka.close()
            except:
                pass
    try:
        suka2 = suka
    except:
        Gtoken().start_requests()
    if "data" in suka:
        list_suka = suka['data']
        suka2 = ""
        for suka in list_suka:
            suka_id = suka['id']
            try:
                suka_nama = str(suka['name'])
            except:
                suka_nama = suka['name'].encode('utf-8')
            try:
                suka_reaksi = str(suka['type'])
            except:
                suka_reaksi = suka['type'].encode('utf-8')
            caption = ""
            desc = ""
            like_id = post_id.split("_")[1]
            like_id = like_id + "_" + suka_id
            created_at = post_tanggal
            bersih = post_tanggal.split('T')[0]
            bersih = bersih.split('-')
            d_year = bersih[0]
            d_month = bersih[0] + bersih[1]
            d_day = bersih[0] + bersih[1] + bersih[2]
            json_like = json.dumps(
                {'type': 'like', 'fb_status_id': posting_id, 'like_from_id': suka_id, 'from': suka_nama,
                 'message': suka_reaksi, 'id': like_id, 'comment_from_id': kosong, 'fb_id': akun_id,
                 'is_comment': 0, 'caption': caption, 'desc': desc, 'created_at': created_at,
                 'd_year': d_year, "crawler_id": id_crawler,
                 'd_month': d_month, 'd_day': d_day})
            hitung += 1
            for kafka in range(0, 20):
                try:
                    prod = KafkaProducer(bootstrap_servers=config.bootstrap_servers)
                    prod.send(config.kafka_topic, json_like)
                    print "======================================="
                    # print json_like
                    print "SELESAI KIRIM KE FB_API"
                    print "LIKE ke - " + str(hitung)
                    print "======================================="
                    break
                except:
                    pass
        if "paging" in suka2:
            if "next" in suka2['paging']:
                situs = suka2['paging']['next']
                dapet_suka(post_id)


if __name__ == '__main__':
    dapet = 0
    count = 0
    Gtoken().start_requests()