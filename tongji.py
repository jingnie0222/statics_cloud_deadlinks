#!/usr/bin/python3
#-*-coding=utf8-*-

import os,sys
import time
import re
import csv
import datetime
import shutil

# Hadoop command prefix
cmd_prefix = "/search/yinjingjing/hadoop-client-mars/bin/hadoop fs -get  /cloud/op/missstore/"


# Mail params
subject = "云图死链检查".encode('utf8').decode('unicode_escape')
body = "./body.txt"
mail_lst = ['yinjingjing@sogou-inc.com', 'hyesung_love@126.com']


def log_error(str):
    sys.stderr.write('%s\n' % str)
    sys.stderr.flush()

def get_dirname_time(path):
    dir_list_time = []
    try:
        file_list = os.listdir(path)
        for filename in file_list:
            if is_number(filename):
                dir_list_time.append(filename)
        return dir_list_time
    except Exception as err:
        log_error('[get_dirname_time]: %s' % err )

def get_dirname_appid(path):
    dir_list_appid = []
    try:
        file_list = os.listdir(path)
        for filename in file_list:
            if is_number(filename):
                dir_list_appid.append(filename)
        return dir_list_appid
    except Exception as err:
        log_error('[get_dirname_appid]: %s' % err ) 

def is_number(dirname):
    try:
        float(dirname)
        return True
    except Exception as err:
        return False
        #log_error('[is_number]: %s' % err )

def get_log(log_dir):
    try:
        if os.path.exists(log_dir):
            shutil.rmtree(log_dir)    
        os.mkdir(log_dir)
        yesterday = (datetime.date.today() - datetime.timedelta(days = 1)).strftime("%Y%m%d")
        month = yesterday[0:6]
        cmd = cmd_prefix + month + "/" + yesterday + "/*" + "  " + log_dir
        os.system(cmd)
        
    except Exception as err:
        log_error('[gen_log] err: %s\n' %  err)

def parse_log(path):
    static_domain_status = dict() 

    files= os.listdir(path)
    for file_name in files:
        with open(path+"/"+file_name, 'r') as f:
            for line in f.readlines(): 
                try:
                    pat_parse_line = re.search(r'HTTP/1.. "(4[0-9][0-9])" [^ ]* [^ ]* "(.*?)".*',line)
                
                    if pat_parse_line:
                        status = pat_parse_line.group(1)
                        domain = pat_parse_line.group(2).replace("https://","").replace("http://","").split('/')[0]
                    
                        if domain not in static_domain_status:
                            static_domain_status[domain] = {"4xx":0,"404":0}
                    
                        static_domain_status[domain]["4xx"] += 1
                        if status == "404":
                            static_domain_status[domain]["404"] += 1
                        
                except Exception as err:
                    log_error('[parse_log]: %s, file:%s' % (err,path+"/"+file_name) )

 
    return static_domain_status

def create_full_dict(log_dir, f_fulldict):
    static_by_hour = dict()
    appid_lst = []
    
    #Sort appid
    dir_time = get_dirname_time(log_dir)
    for hour in dir_time:
        dir_appid = get_dirname_appid(log_dir+"/"+hour)
        for appid in dir_appid:
            if int(appid) not in appid_lst:
                appid_lst.append(int(appid))
    appid_lst.sort()
    
    #Sort time
    dir_time = [int(time) for time in dir_time]
    dir_time.sort()
    
    for appid in appid_lst:
        for hour in dir_time:
            logfile_path = log_dir + "/" + str(hour) + "/" + str(appid)
            if not os.path.isdir(logfile_path):
                continue
            domain_status = parse_log(logfile_path)
            if not domain_status:
                continue
            if appid not in static_by_hour:
                static_by_hour[appid] = dict()
            static_by_hour[appid][hour] = domain_status

            #Print full dict
            for domain in static_by_hour[appid][hour]:
                f_fulldict.write('appid:%10s \t time:%10s \t domain:%30s \t 4xx:%10d \t 404:%10d\n\n' % (appid, hour, domain, domain_status[domain]["4xx"],domain_status[domain]["404"]))


def get_value(fulldict_line):
    pat_content = re.search(r'appid:(.*?)\ttime:(.*?)\tdomain:(.*?)\t4xx:(.*?)\t404:(.*)', fulldict_line.replace(' ',''))
    if pat_content:
        appid =  pat_content.group(1)
        hour = pat_content.group(2)
        domain = pat_content.group(3)
        single_count_4xx = int(pat_content.group(4))
        single_count_404 = int(pat_content.group(5))
        return appid,hour,domain,single_count_4xx,single_count_404

        
def gen_result_dir(result_dir_prefix):
    try:
        today = datetime.date.today().strftime("%Y%m%d")
        os.mkdir(result_dir_prefix+"_"+today)
    except FileExistsError:
        log_error('[gen_result_dir] Dir exists: %s. remove dir, mkdir again' % (result_dir_prefix+"_"+today))
        shutil.rmtree(result_dir_prefix+"_"+today)
        os.mkdir(result_dir_prefix+"_"+today)
        
    result_dir = result_dir_prefix+"_"+today
    return result_dir   

def int_to_time(str):
    # Hour
    if len(str) == 10:
        time_str = time.strftime("%Y-%m-%d %H:%M:%S",time.strptime(str,"%Y%m%d%H"))
    # Day
    if len(str) == 8:
        time_str = time.strftime("%Y-%m-%d",time.strptime(str,"%Y%m%d"))
    return time_str    
        
def static_by_appid(source_file, f_hour, f_day):
    
    static_deadlinks_by_hour = dict()

    with open(source_file, 'r') as f:
        for line in f.readlines():
            if not get_value(line):
                continue

            appid,hour,domain,single_count_4xx,single_count_404 = get_value(line)
            # A new appid
            if appid not in static_deadlinks_by_hour:
                static_deadlinks_by_hour[appid] = dict()
            # A new hour
            if hour not in static_deadlinks_by_hour[appid]:
                static_deadlinks_by_hour[appid][hour] = {"4xx": 0, "404": 0}

            # Count++
            static_deadlinks_by_hour[appid][hour]["4xx"] += single_count_4xx
        static_deadlinks_by_hour[appid][hour]["404"] += single_count_404
    
    headers = ['appid', 'time', 'count_4xx', 'cout_404']
    f_hour.writerow(headers)
    f_day.writerow(headers)
     
    for appid in static_deadlinks_by_hour:
        lastday = "00000000"

        for hour in static_deadlinks_by_hour[appid]:
            day = hour[0:8]
            f_hour.writerow((appid, int_to_time(hour), static_deadlinks_by_hour[appid][hour]["4xx"], static_deadlinks_by_hour[appid][hour]["404"]))

            if lastday != day:
                if lastday != "00000000":
                    f_day.writerow((appid, int_to_time(lastday), static_deadlinks_day_4xx, static_deadlinks_day_404))
                static_deadlinks_day_4xx = 0
                static_deadlinks_day_404 = 0

            static_deadlinks_day_4xx += static_deadlinks_by_hour[appid][hour]["4xx"]
            static_deadlinks_day_404 += static_deadlinks_by_hour[appid][hour]["404"]
            lastday = day

        if  lastday != "00000000":
            f_day.writerow((appid, int_to_time(lastday), static_deadlinks_day_4xx, static_deadlinks_day_404))

    return None

def static_by_domain(source_file, f_hour, f_day):

    static_domain_by_hour = dict()
    
    with open(source_file, 'r') as f:
        for line in f.readlines():
            if not get_value(line):
                continue

            appid,hour,domain,single_count_4xx,single_count_404 = get_value(line)
                 
            # A new domain
            if domain not in static_domain_by_hour:
                static_domain_by_hour[domain] = dict()
            # A new hour
            if hour not in static_domain_by_hour[domain]:
                static_domain_by_hour[domain][hour] = {"4xx": 0, "404": 0}

            # Count++
            static_domain_by_hour[domain][hour]["4xx"] += single_count_4xx
            static_domain_by_hour[domain][hour]["404"] += single_count_404

    headers = ['domain', 'time', 'count_4xx', 'cout_404']
    f_hour.writerow(headers) 
    f_day.writerow(headers)    
        
    for domain in static_domain_by_hour:
        lastday = "00000000"

        for hour in static_domain_by_hour[domain]:
            day = hour[0:8]
            f_hour.writerow((domain, int_to_time(hour), static_domain_by_hour[domain][hour]["4xx"], static_domain_by_hour[domain][hour]["404"]))     

            if lastday != day:
                if lastday != "00000000":
                    f_day.writerow((domain, int_to_time(lastday), static_domain_day_4xx, static_domain_day_404 ))     
                static_domain_day_4xx = 0
                static_domain_day_404 = 0

            static_domain_day_4xx += static_domain_by_hour[domain][hour]["4xx"]
            static_domain_day_404 += static_domain_by_hour[domain][hour]["404"]
            lastday = day

        if  lastday != "00000000":
            f_day.writerow((domain, int_to_time(lastday), static_domain_day_4xx, static_domain_day_404))

    return None

def sendmail(subject, body, att_lst, mail_lst):
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
    #Send mail
    os.system(cmd_sendmail)

def main():
    try:
        log_dir = 'hadoop_log'
        result_dir_prefix = 'test_result'
        
        #Get log from hadoop
        get_log(log_dir)
        
        #Make result dir
        result_dir = gen_result_dir(result_dir_prefix)
        
        # Save full dict
        with open(result_dir+"/full_dict", 'w+') as f_fulldict:
            create_full_dict(log_dir, f_fulldict)
            
        #Static by appid, Save as csv
        with open(result_dir+"/static_by_hour.csv", 'w') as f_hour:
            f_hour_csv = csv.writer(f_hour)
            with open(result_dir+"/static_by_day.csv", 'w') as f_day:
                f_day_csv = csv.writer(f_day)
                static_by_appid(result_dir+"/full_dict", f_hour_csv, f_day_csv)
        
        #Static by domain, Save as csv
        with open(result_dir+"/static_domain_by_hour.csv", 'w') as f_hour:
            f_hour_csv = csv.writer(f_hour)
            with open(result_dir+"/static_domain_by_day.csv", 'w') as f_day:
                f_day_csv = csv.writer(f_day)
                static_by_domain(result_dir+"/full_dict", f_hour_csv, f_day_csv)
        
        #Send Mail
        att_lst = []
        res_file_lst = os.listdir(result_dir)
        for res_file in res_file_lst:
            if re.match('static', res_file):
                att_lst.append(result_dir + "/" +res_file)
 
        sendmail(subject, body, att_lst, mail_lst)
        
    except Exception as err:
        log_error('[main] %s' % err)

if __name__ == '__main__':
    main()
    print('Done')
    
    
        
        
    





