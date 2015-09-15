import MySQLdb
import socket

class MyProxyDownloadMiddleware(object):
    
    def process_request(self,request,spider):
        conn = cursor.connect(host='localhost',user='root',passwd='hansen',db='popu')
        cursor = conn.cursor()
        proxyip = ''
        
        
        #weight is default inited to set 8000, when used ,weight decrease 1 
        
        while True:
            ret = cursor.execute('select ptype, ip,port,id,weight from proxyip order by weight desc and bvalid = 1 limit 1')
            record = cursor.fetchone()
            ptype = record[0]
            ip = record[1]
            port = record[2]
            id = record[3]
            weight = record[4]
            
            sfd = socket.socket(socket.AF_INET,socket.SOCK_TREAM)
            sfd.settimeout(3)
            try:
                tup = ip,port
                sfd.connect(tup)
                proxyip = str(ptype) + r'://' + str(ip) + r':' + str(port)
                request.meta['proxy'] = proxyip
                
                cursor.execute('update proxyip set weight=%d where id=%d'%(weight-1,id))
                cursor.commit()
                
                break
                
            except:
                sfd.close()
                cursor.execute('update proxyip set bvalid = 0 where id=%d',id)
                cursor.commit()
                
                print 'connect proxy failed!'
                
                continue
            
            
        
        
        
        
        