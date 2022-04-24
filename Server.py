#!/usr/bin/python3
import rpyc
from rpyc.utils.server import ThreadedServer
import datetime
from rpyc.utils.registry import REGISTRY_PORT, DEFAULT_PRUNING_TIMEOUT
from rpyc.utils.registry import UDPRegistryServer
from rpyc.utils.factory import DiscoveryError
from threading import Thread
import threading
from time import sleep
import numpy as np
import sys
import socket
import os
import signal

STATE={0: "NF" , 1 : "F" }
ACTION={0: "undefine" , 1 : "attack",2 : "retreat"}
CONVERGE={0:"STABLE",1:"CONVERGING"}
class RAService(rpyc.Service):
    class backend(Thread):
        def __init__(self,_id,nodes,ip,port,leader):
            Thread.__init__(self)
            self.STATE=0
            self.ACTION=0
            self.CONVERGE=1
            self.LEADER=leader
            self._id=_id
            self.RANodes=[]
            self.nodes=nodes
            self.seeds={ i:0 for i in range(self.nodes) }
            self.nodes=nodes
            self.waitForResponse=0
            self.waitForReply=[]
            self.receivedFromLeader=False
            self.replyResult=[]
            self.waitForOrderReply=[]
            self.ip = ip
            self.port = port


        def run(self):
            while True:
                try:
                    self.RANodes = rpyc.discover("RA")
                    self.nodes = len(self.RANodes)
                    sleep(2)
                except DiscoveryError:
                        pass

    # end of inner class

    def __init__(self,_id,nodes,ip,port,leader):
        super(self.__class__, self).__init__()
        self.callback = None
        self.b = self.backend(_id, nodes, ip, port,leader)
        self.b.start()

    def exposed_order(self,action,level,src):
        if(level==1 and self.b.receivedFromLeader==False):
            #print(f'{self.b._id} : Append  {src}')
            self.b.waitForOrderReply.append(src)
            return
        if(level<1):
            self.b.ACTION=action
            self.b.waitForReply.append(src)
            self.b.receivedFromLeader=True
            r = self.b.nodes-(2+level)
            if(level==-1): r=0
            self.b.waitForResponse=(r)
            print(f'{self.b._id} : order L:{level}, S:{src}, A:{action}, R:{self.b.waitForResponse}')
            for i in self.b.RANodes:
                if( i[1]!=self.b.port and i[1]!=self.b.LEADER):
                    conn = rpyc.connect(i[0],i[1])
                    if(self.b.STATE==1):
                        action=np.random.randint(1,3)
                    if(level==-1):
                        self.b.replyResult.append(action)
                    req = rpyc.async_(conn.root.order)
                    req(action,level+1,self.b.port)
            for src in self.b.waitForOrderReply:
               # print(f'{self.b._id} : Reply(delay) to {src}')
                action = self.b.ACTION
                conn = rpyc.connect(self.b.ip,src)
                req = rpyc.async_(conn.root.reply)
                if(self.b.STATE==1):
                    action=np.random.randint(1,3)
                if(self.b.nodes<4):
                    action=0
                req(action)
            if(level==-1):
                self.exposed_reply(-1)
        else:
            #print(f'{self.b._id} : Reply to {src}')
            action = self.b.ACTION
            conn = rpyc.connect(self.b.ip,src)
            req = rpyc.async_(conn.root.reply)
            if(self.b.STATE==1):
                action=np.random.randint(1,3)
            if(self.b.nodes<4):
                action=0
            req(action)

    def exposed_reply(self,action):
        if(self.b.waitForResponse>0):
            self.b.waitForResponse-=1
            self.b.replyResult.append(action)
            #print(f'{self.b._id} : {self.b.waitForResponse}')
        if(self.b.waitForResponse==0):
            self.b.replyResult.append(self.b.ACTION)
            #result = int(np.bincount(self.b.replyResult).argmax())
            print(f'{self.b._id} : {self.b.replyResult}')
            self.callback(self.b._id,self.b.STATE,self.b.nodes,self.b.replyResult)
            self.b.replyResult=[]
            self.b.waitForReply=[]
            self.b.waitForOrderReply=[]

    def exposed_leader(self):
        return self.b.LEADER

    def exposed_detail(self):
        return self.b._id,self.b.STATE,self.b.LEADER

    def exposed_setState(self,state):
        self.b.STATE=state

    def exposed_setLeader(self,leaderport):
        self.b.LEADER = leaderport

    def exposed_clearFlag(self):
        self.b.receivedFromLeader=False

    def exposed_setCallBack(self,obj=None):
        self.callback = rpyc.async_(obj)

    def exposed_kill(self):
        pool = threading.enumerate()
        for t in pool:
            if(type(t._target)!=type(None)):
                obj=t._target.__self__
                if(isinstance(obj,ThreadedServer)):
                    if(obj.port==self.b.port):
                        print(f'{self.b._id} Kill')
                        obj.close()

    def exposed_newNode(self,k):
        maxid=0
        leaderport=0
        for node in self.b.RANodes:
            conn=rpyc.connect(node[0],node[1])
            result = conn.root.detail()
            if(result[0]>maxid):
                maxid=result[0]
            leaderport=result[2]
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        ports = np.random.randint(1024,65535,k)
        for i in range(1,k+1):
            service = RAService(maxid+i,self.b.nodes,ip,int(ports[i-1]),leaderport)
            server=ThreadedServer(service, port=int(ports[i-1]),auto_register=True)
            t = Thread(target=server.start,name=str(maxid+i))
            t.daemon=True
            t.start()




class RegistryService():
    def __init__(self):
        self.server = UDPRegistryServer(port=REGISTRY_PORT,pruning_timeout=DEFAULT_PRUNING_TIMEOUT)

    def start(self):
        self.server.start()

if __name__=='__main__':
    if(len(sys.argv)<2):
        print("Missing Args")
        sys.exit(1)
    N = int(sys.argv[1])
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    registry = RegistryService()
    t=Thread(target=registry.start)
    t.daemon=True
    t.start()
    ports = np.random.randint(1024,65535,N)
    leader = np.argmax(ports)
    print(leader)
    print(ports)
    for i in range(N):
        service = RAService(i,N,ip,int(ports[i]),ports[leader])
        server=ThreadedServer(service, port=int(ports[i]),auto_register=True)
        t = Thread(target=server.start,name=str(i))
        t.daemon=True
        t.start()



