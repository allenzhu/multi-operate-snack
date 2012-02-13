#!/usr/bin/env python

import Queue
import sys
import threading
import time
import getpass
import optparse
import os
from subprocess import Popen,PIPE

#sys.path.insert(0, "/usr/lib64/python2.4")
#sys.path.insert(0, "/usr/local/python-2.7.2/lib/python2.7/site-packages")
#sys.path.insert(0, "/usr/local/python-2.7.2/lib/python2.7")
#sys.path.insert(0, "/usr/src/kernels/2.6.18-164.el5-xen-x86_64/include/config")
#sys.path.insert(0, "/usr/taobao/lib")
#sys.path.insert(0, "/usr/local/python-2.7.2/lib/python2.7/site-packages")

#print "---------"
#print sys.path
#print "---------"

import paramiko

class Worker(threading.Thread):
        worker_count=0
        def __init__(self,workQueue,resultQueue):
                threading.Thread.__init__(self)
                self.id=Worker.worker_count
                Worker.worker_count+=1
                self.setDaemon(True)
                self.workQueue=workQueue
                self.resultQueue=resultQueue

        def run(self):
                while True:
                        try:
                                cal,args=self.workQueue.get(timeout=2)
                                res=cal(*args)
                                #print "worker[%2d]:%s"%(self.id,str(res))
                                self.resultQueue.put(res)
                        except Queue.Empty:
                                #print 'empty queue'
                                break
                        except :
				print 'worker[%2d]' % self.id,sys.exc_info()[:2]


class WorkerManager:
        def __init__(self,num_of_workers):
                self.workQueue=Queue.Queue()
                self.resultQueue=Queue.Queue()
                self.workers=[]
                self._recruitThreads(num_of_workers)

        def _recruitThreads(self,num_of_workers):
                for i in range(num_of_workers):
                        worker=Worker(self.workQueue,self.resultQueue)
                        self.workers.append(worker)

        def start(self):
                for w in self.workers:
                        w.start()

        def wait_for_complete(self):
                while len(self.workers):
                        worker=self.workers.pop()
                        worker.join()
                        if worker.isAlive() and not self.workQueue.empty():
                                self.workers.append(worker)
                print "all finished."
	
	def add_job(self,cal,*args):
                self.workQueue.put((cal,args))

        def get_result(self,*args):
                return self.resultQueue.get(*args)

def common_parser():
	parser=optparse.OptionParser(conflict_handler='resolve')
	parser.add_option('-u','--user',dest='user',help='ssh user')
	parser.add_option('-p','--password',dest='pwd',help='ssh password',action='store_true')
	parser.add_option('-h','--help',dest='help',action='store_true')
	parser.add_option('-f','--addressfile',dest='file',help='address file')
	parser.add_option('-g','--group',dest='group',help="server group")
	parser.add_option('-A','--add',dest='add',action='store_true',default=False)
	parser.add_option('-C','--Concurrent',dest='concurrent',default=10,help='number of concurrent')
	parser.usage="usage: snack [OPTIONS] 'command'"
	parser.usage+="\n-u,--user		ssh user,default is current user"
	parser.usage+="\n-p,--password		password"
	parser.usage+="\n-h,--help		help information"
	parser.usage+="\n-f,--addressfile	server address file , which is seperated by \\n"
	parser.usage+="\n-g,--group		server group"
	parser.usage+="\n-A,--add		whether statistic total unit"
	parser.usage+="\n-C,--Concurrent	number of concurrent"
	parser.usage+="\nFor example:"
	parser.usage+="\n	snack -p -u user -f servers.txt 'w'"
	parser.usage+="\n	snack -p -u user -g XXXhost 'w'"
	parser.usage+="\n	snack -p -g XXXhost 'w'"
	parser.usage+="\n	snack -p -f addFile --Add 'cat file| wc -l'"
	parser.usage+="\n	snack -p -f addFile -C 8 'hostname'"
	return parser

def checkConcurrent(opts,parser):
        try:
		tmp=int(opts.concurrent)
                if tmp<1:
                        parser.error("Concurrent number is invalid.")
                        sys.exit()
        except:
                parser.error("Concurrent number is invalid.")
                sys.exit()

def gethosts(opts,parser):
	hosts=[]
	if opts.file is not None:
		f=open(opts.file,'r')
		hosts=f.readlines()
	else:
		if opts.group is not None:
			f=Popen(('opsfree','-l','-g',opts.group),stdout=PIPE).stdout
			hosts=f.readlines()
		else:
			parser.error("Please input hosts. -f or -g options")
			sys.exit()
	return hosts

def getuser(opts):
	user=""
	if opts.user is not None:
		user=opts.user
	else:
		f=Popen(('whoami'),stdout=PIPE).stdout	
		user=f.readline().strip()
	return user

def getpwd(opts,parser):
	if opts.pwd is None:
		parser.error("Please add -p options, it do not support tunnel, maybe later.")
		sys.exit()
	pwd=getpass.getpass('Password:')
	return pwd

def format(l,host):
	start='<'+host+'> start'
	end='<'+host+'> end'
	if has_colors(sys.stdout):
		start=g('%s' % B(start))
		end=g('%s' % B(end))
	fs=start+'\n'
	for seg in l:
		fs+=seg
	fs+='\n'+end
	print fs

def job(host,user,pwd,command,needret):
	try:
		ssh=paramiko.SSHClient()
		#known_hosts=os.path.expanduser('~/.ssh/known_hosts')
		#ssh.load_system_host_keys(known_hosts)
		ssh.load_system_host_keys()
		ssh.set_missing_host_key_policy(paramiko.WarningPolicy())
		#print '*** Connecting ....'
		ssh.connect(host,username=user,password=pwd)
		stdin,stdout,stderr=ssh.exec_command(command)
		l=stdout.readlines()
		format(l,host)
		ssh.close()
		#print 'end ...'
		if needret and len(l)>0:
			return str(l[0]).strip()
	except Exception,data:
		print Exception,"@",data

def with_color(str,fg,bg=49):
	return "\x1b[%dm\x1b[%dm%s\x1b[39m\x1b[49m" % (fg,bg,str)

def B(str):
	return "\x1b[1m%s\x1b[22m" % str

def r(str): return with_color(str, 31) # Red
def g(str): return with_color(str, 32) # Green
def y(str): return with_color(str, 33) # Yellow
def b(str): return with_color(str, 34) # Blue
def m(str): return with_color(str, 35) # Magenta
def c(str): return with_color(str, 36) # Cyan
def w(str): return with_color(str, 37) # White

#following from Python cookbook, #475186
def has_colors(stream):
    '''Returns boolean indicating whether or not the supplied stream supports
    ANSI color.
    '''
    if not hasattr(stream, "isatty"):
        return False
    if not stream.isatty():
        return False # auto color only on TTYs
    try:
        import curses
        curses.setupterm()
        return curses.tigetnum("colors") > 2
    except:
        # guess false in case of error
        return False

def main():
	parser=common_parser()
	opts,args=parser.parse_args()
	if opts.help==True:
		print parser.usage
		sys.exit()
	if len(args)==0 :
		parser.error("Command not specified.")
		print parser.usage
	if opts.pwd is None:
		parser.error("Please input password. -p options")
		print parser.usage
	checkConcurrent(opts,parser)
	hosts=[]
	hosts=gethosts(opts,parser)
	user=getuser(opts)
	pwd=getpwd(opts,parser)
	
	wm=WorkerManager(int(opts.concurrent))
	for h in hosts:
		wm.add_job(job,h.strip(),user,pwd,args[0],opts.add)
	wm.start()
	wm.wait_for_complete()
	
	#job(hosts[0].strip(),user,pwd,args[0])
	#print 'all job done.'
	if opts.add:
		sum=0
		while not wm.resultQueue.empty():
			sum+=int(wm.get_result())
		print 'total:',sum


if __name__=="__main__":
	main()

