#!/usr/bin/python3
#-*-coding=utf8-*-
import datetime
import os,sys
import shutil
from dateutil.parser import parse
import time


store_log_path = "/search/yinjingjing/yuntu2"
cmd_prefix = "/search/yinjingjing/hadoop-client-mars/bin/hadoop fs -get  /cloud/op/missstore/"

def utf8stdout(in_str):
    utf8stdout = open(1, 'w', encoding='utf-8', closefd=False) # fd 1 is stdout
    print(in_str, file=utf8stdout)

def log_error(str):
    sys.stderr.write('%s\n' % str)
    sys.stderr.flush()
    
'''
def sendmail(subject, body,att_lst, mail_lst):
    cmd_sendmail = ""
    att_str = ""
    mail_str = ""
    
    #Subject
    cmd_sendmail = "mutt -s " + '"' + subject + '"'
    #Mail body
    cmd_sendmail = cmd_sendmail + " -i " + body
    #Attachment
    for att in att_lst:
        att_str = " -a " + att
        cmd_sendmail += att_str
    cmd_sendmail += " -F ./mutt_config < /dev/null"
    #Addressee
    for mailto in mail_lst:
        mail_str = mail_str + " " + mailto
    cmd_sendmail = cmd_sendmail + " -- " + mail_str
    #
    os.system(cmd_sendmail)

subject = "云图死链检查".encode('utf8').decode('unicode_escape')
print(subject)
body = "./body.txt"
mail_lst = ['yinjingjing@sogou-inc.com', 'hyesung_love@126.com']
att_lst = ['static_by_hour.csv', 'static_by_day.csv', 'static_domain_by_hour.csv', 'static_domain_by_day.csv', 'body.txt']
sendmail(subject, body, att_lst, mail_lst)
'''    

'''
def get_log(log_dir):
    try:
        if os.path.exists(log_dir):
            shutil.rmtree(log_dir)    
        os.mkdir(log_dir)
        yesterday = (datetime.date.today() - datetime.timedelta(days = 1)).strftime("%Y%m%d")
        month = yesterday[0:6]
        cmd = cmd_prefix + month + "/" + yesterday + "/*" + "  " + log_dir
        #os.system(cmd)
        
    except Exception as err:
        log_error('[gen_log] err: %s\n' %  err)

get_log('hadoop_log')


#result_path =  "/search/yinjingjing/cloud_result"


def gen_result_dir(result_dir_prefix):
    try:
        today = datetime.date.today().strftime("%Y%m%d")
        os.mkdir(result_dir_prefix+"_"+today)
    except FileExistsError:
        log_error('[gen_result_dir] Dir exists: %s.\t remove dir, mkdir again' % (result_dir_prefix+"_"+today))
        shutil.rmtree(result_dir_prefix+"_"+today)
        os.mkdir(result_dir_prefix+"_"+today)
        
gen_result_dir('test_result')
'''


def int_to_time(str):
    if len(str) == 10:
        time_str = time.strftime("%Y-%m-%d %H:%M:%S",time.strptime(str,"%Y%m%d%H"))
    if len(str) == 8:
        time_str = time.strftime("%Y-%m-%d",time.strptime(str,"%Y%m%d"))
    return time_str

print(int_to_time(str(2018112000)))
print(int_to_time(str(20181120)))
        
