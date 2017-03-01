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
# from ssdb import SSDB


####################     Generate Token      ###############################################
class Gtoken():
    def __init__(self):
        global driver
        self.conn = MySQLdb.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            passwd=config.passwd,
            db=config.db)
        self.connect = self.conn
        myProxy = config.firefox_proxy
        proxy = Proxy({
            'proxyType': ProxyType.MANUAL,
            'httpProxy': myProxy,
            'ftpProxy': myProxy,
            'sslProxy': myProxy,
        })
        display = Display(visible=0, size=(1352, 780))
        display.start()
        driver = webdriver.Firefox()  # proxy=proxy)

    # driver = webdriver.PhantomJS()
    def start_requests(self):
        global token, parameter,account, cout, inisial, ssdb
        # ssdb = SSDB(host='localhost', port=8888)
        token = config.token1
        cur = self.conn.cursor()
        cou = self.conn.cursor()
        try:
            cout = "select count(*)from malaysia where api ='3'"
            sql = "select url from malaysia where api ='3'"
            cur.execute(sql)
            cou.execute(cout)
            import pdb;pdb.set_trace()
            results = cur.fetchall()
            b = cou.fetchall()
            terus = str(b).replace(",", "").replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace(
                "]", "").replace("L", "")
            print (terus)
            terus = int(terus)
            print "============================================"
            print (terus)
            cout = 0
            for ulang in range(0, terus):
                try:
                    print (ulang)
                    cout += 1
                    a = results[ulang]
                    account = str(a).replace(",", "").replace("'", "").replace("(", "").replace(")", "")
                    inisial = account
                    sql = "select api from malaysia where URL='{}'".format(account)
                    cur.execute(sql)
                    parameter = cur.fetchall()
                    parameter = str(parameter).replace(",", "").replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", "").replace("L", "")
                    api = "3"
                    sql = "UPDATE malaysia SET api = '{}' WHERE URL = '{}'".format(api, inisial)
                    cur.execute(sql)
                    self.conn.commit()
                    try:
                        try:
                            acun = re.findall('\d+', account)
                            acun = ''.join(acun)
                            int(acun)
                            account = acun
                        except:
                            if "pages" in account:
                                account = account.split('pages/')[1]
                                account = account.replace("/", "")
                            else:
                                account = account.split('.com/')[1]
                                account = account.replace("/", "")
                        str(account)
                        akun = account
                        url = ""
                        count = 0
                        print account
                        try:
                            dapet_post(akun, url)
                        except:pass
                    except:
                        pass
                except:
                    pass
        except Exception, e:
            if cout < 2 :
                api = "fail"
                sql = "UPDATE malaysia SET api = '{}' WHERE URL = '{}'".format(api, inisial)
                cur.execute(sql)
                self.conn.commit()
            self.conn.close()
        self.conn.close()


####################     FB_API      ###############################################

def dapet_post(akun, url):
    global dapet, count, itungan, post_tanggal, ling, ling, title, title,kosong,akun_id,posting_id,id_crawler,perbandingan

    id_crawler = config.crawler_id
    url3 = "https://graph.facebook.com/v2.8/" + akun + "?access_token=" + token
    try:
        for loop in range(0, 20):
            try:
                request_nama = urllib2.urlopen(url3)
                nama = json.loads(request_nama.read())
                nama_asal = nama['name']
                id = nama['id']
                request_nama.close()
                break
            except:
                try:
                    request_nama.close()
                except:
                    pass
        if url == "":
            url = "https://graph.facebook.com/v2.8/" + akun + "/feed?access_token=" + token
        else:
            url = url
        for loop in range(0, 20):
            try:
                request_data = urllib2.urlopen(url)
                data = json.loads(request_data.read())
                request_data.close()
                break
            except:
                try:
                    request_data.close()
                except:
                    pass
        url2 = "https://graph.facebook.com/v2.8/" + akun + "/feed?fields=type&access_token=" + token
        for loop in range(0, 20):
            try:
                request_tipe = urllib2.urlopen(url2)
                tipe = json.loads(request_tipe.read())
                request_tipe.close()
                break
            except:
                try:
                    request_tipe.close()
                except:
                    pass

        try:
            simpan = data
            tipe = tipe
        except:
            return
        if request_data != "":
            if "data" in data:
                list_post = data['data']
                list_tipe = tipe['data']
                # print list_post[1]
                # count_post = count_post + int(len(list_post))
                for data, tipe in zip(list_post, list_tipe):
                    print "======================================="
                    print "AMBIL POST"
                    print "======================================="
                    post_id = data['id']
                    fb_type = tipe['type']
                    post_id2 = post_id.split("_")
                    akun_id = post_id2[0]
                    post_nama = akun
                    try:
                        post_pesan = str(data['message'])
                    except:
                        try:
                            post_pesan = data['message'].encode('utf-8')
                        except:
                            try:
                                post_pesan = str(data['story'])
                            except:
                                try:
                                    post_pesan = data['story'].encode('utf-8')
                                except:
                                    post_pesan = "-"
                    try:
                        url2 = "https://graph.facebook.com/v2.8/" + post_id + "/attachments?access_token=" + token
                        for loop in range(0, 20):
                            try:
                                request_attach = urllib2.urlopen(url2)
                                break
                            except:
                                pass
                        attach = json.loads(request_attach.read())
                        simpan1 = attach
                        request_attach.close()
                        if "data" in attach:
                            list_attach = attach['data']
                            try:
                                ling = attach['url']
                                title = attach['title']
                            except:
                                title = ""
                                ling = ""
                    except:
                        pass
                    url3 = "https://graph.facebook.com/v2.8/" + akun_id + "?access_token=" + token
                    for loop in range(0, 20):
                        try:
                            request_nama = urllib2.urlopen(url3)
                            nama = json.loads(request_nama.read())
                            request_nama.close()
                            break
                        except:
                            try:
                                request_nama.close()
                            except:
                                pass
                    from_nama = nama['name']
                    if from_nama != nama_asal:
                        id_akun = akun_id
                    else:
                        id_akun = ""
                    post_tanggal = data['created_time']
                    bersih = post_tanggal.split('T')[0]
                    bersih = bersih.split('-')
                    #ambil perbandingan
                    banding = post_tanggal.split('T')[1]
                    banding = banding.split(':')[0]
                    now = datetime.datetime.now()
                    coy = now.strftime("%H")
                    coy_hari = now.strftime("%d")
                    # if count == 25:
                    #     import pdb;pdb.set_trace()
                    d_year = bersih[0]
                    kosong = ""
                    posting_id = post_id.split("_")[1]
                    d_month = bersih[0] + bersih[1]
                    d_day = bersih[0] + bersih[1] + bersih[2]
                    hari = bersih[2]
                    perbandingan = int(coy) - int(banding)
                    # perbandingan = abs(perbandingan)
                    if perbandingan > 2 and hari == coy_hari:
                        batas = "true"
                    else:
                        batas = "false"
                    #
                    json_post = json.dumps({'type': 'post', 'fb_id': id, 'id': post_id, 'from': from_nama,
                                            'fb_status_id':posting_id,'comment_from_id':id_akun,'like_from_id':kosong,
                                            'caption': ling, 'desc': post_pesan, 'created_at': post_tanggal,
                                            'fb_type': fb_type, 'crawler_id':id_crawler,
                                            'd_year': d_year, 'd_month': d_month, 'd_day': d_day, 'is_comment': 0,
                                            'message': title})
                    # raw = "raw"+str(count)
                    # count += 1
                    # ssdb.setx(raw, post_id, 12800)
                    for kafka in range(0, 20):
                        try:
                            # producer = KafkaProducer(bootstrap_servers=config.bootstrap_servers)
                            # producer.send(config.kafka_topic, json_post)
                            print "======================================="
                            print post_id
                            print json_post
                            print "SELESAI KIRIM KE FB_API"
                            count +=1
                            print "======================================="
                            break
                        except:
                            pass
                    itungan = 0
                    # dapet_komen(post_id)
                    hitung = 0
                    # dapet_suka(post_id)
                    if batas == "false":
                        break
                if batas == 'true':
                    if "paging" in simpan:
                        if "next" in simpan['paging']:
                            url = simpan['paging']['next']
                            itungan = 0
                            dapet_post(akun, url)
                    else:
                        count = 0
                        pass
                else:pass
    # Gtoken.start_requests()
            return dapet
        else:
            pass
    except:pass
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
                 'id': com_id,'fb_id':akun_id,'like_from_id':kosong,"crawler_id":id_crawler,
                 'message': comment_pesan, 'created_at': comment_tanggal, 'desc': desc, 'is_comment': 1,
                 'caption': caption, 'd_year': d_year, 'd_month': d_month, 'd_day': d_day})
            for timpa in range(len(config.ray)):
                json_comment = json_comment.replace(config.ray[timpa], "")
            # json_comment = json.dumps({'type' : 'comment', 'id' : comment_id, 'nama' : comment_nama, 'post_id' : post_id, 'message' : comment_pesan, 'tanggal_dibuat' : comment_tanggal})
            for kafka in range(0, 20):
                try:
                    # prod = KafkaProducer(bootstrap_servers=config.bootstrap_servers)
                    # prod.send(config.kafka_topic, json_comment)
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
            json_like = json.dumps({'type': 'like', 'fb_status_id': posting_id, 'like_from_id': suka_id, 'from': suka_nama,
                                    'message': suka_reaksi, 'id': like_id,'comment_from_id':kosong,'fb_id':akun_id,
                                    'is_comment': 0, 'caption': caption, 'desc': desc, 'created_at': created_at,
                                    'd_year': d_year,"crawler_id":id_crawler,
                                    'd_month': d_month, 'd_day': d_day})
            hitung += 1
            for kafka in range(0, 20):
                try:
                    # prod = KafkaProducer(bootstrap_servers=config.bootstrap_servers)
                    # prod.send(config.kafka_topic, json_like)
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