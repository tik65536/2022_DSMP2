#!/usr/bin/python3
import rpyc
from time import sleep
from rpyc.utils.registry import REGISTRY_PORT, DEFAULT_PRUNING_TIMEOUT
from rpyc.utils.registry import UDPRegistryClient
from rpyc.utils.factory import DiscoveryError
import sys
import random
import numpy as np

cmd = sys.argv[1]
STATE={"nonfaulty":0 , "faulty": 1 }
STATE2={0:"NF" , 1:"F" }
ACTION={"attack":1, "retreat":2}
ACTION2={0:"undefine",1:"attack", 2:"retreat"}
CONVERGE={0:"STABLE",1:"CONVERGING"}
callback=0
callbackresult=[]

def resultReady(_id,state,nodes,lst):
    global callback,callbackresult
    if(len(lst)>0):
        callbackresult.append([_id,state,nodes,lst])
    callback-=1

if(cmd.lower()=="actual-order"):
    if(sys.argv[2].lower()=="attack" or sys.argv[2].lower()=="retreat"):
        bgservice=[]
        try:
            registrar = UDPRegistryClient(port=REGISTRY_PORT)
            list_of_servers = registrar.discover("RA")
            print(f'length of list {len(list_of_servers)}')
            print('Start set callback')
            for server in list_of_servers:
                conn = rpyc.connect(server[0],server[1])
                bgservice.append(rpyc.BgServingThread(conn))
                conn.root.setCallBack(resultReady)
                callback+=1
            print('Finish set callback')
            leaderport = int(conn.root.leader())
            conn = rpyc.connect(list_of_servers[0][0],leaderport)
            conn.root.order(ACTION[sys.argv[2]],-1,-1)
            while(callback>0):
                print(callback)
                sleep(1)
            for server in list_of_servers:
                conn = rpyc.connect(server[0],server[1])
                conn.root.clearFlag()
            finalOrder=[]
            state=[]
            for i in callbackresult:
                count = np.bincount(i[3])
                action = count.argmax()
                finalOrder.append(action)
                state.append(i[1])
                print(f'{i[0]},majority={ACTION2[action]},state={STATE2[i[1]]}')
            count = np.bincount(finalOrder)
            finalOrder = count.argmax()
            state = np.where(np.array(state)==1)[0].shape[0]
            print(f'Execute order:{ACTION2[finalOrder]}, {state} faulty node in System, {count.max()-1-state}/{len(callbackresult)} quorum suggest {ACTION2[finalOrder]}')
        except DiscoveryError:
            print(f"DiscoveryError :{DiscoveryError}")
if(cmd.lower()=="g-state"):
    l = len(sys.argv)
    try:
        registrar = UDPRegistryClient(port=REGISTRY_PORT)
        list_of_servers = registrar.discover("RA")
        resultlist = []
        for server in list_of_servers:
            conn = rpyc.connect(server[0],server[1])
            result = conn.root.detail()
            p="PRIMARY"
            if(result[2]!=server[1]):
                p="SECONDARY"
            resultlist.append(f'ID:{result[0]},{p},state={STATE2[result[1]]}')
            if(l>2 and result[0]==int(sys.argv[2])):
                try:
                    conn.root.setState(STATE[sys.argv[3]])
                    sys.exit(0)
                except KeyError:
                    print('Input State not valid')
                    sys.exit(1)
        for l in resultlist:
            print(l)

    except DiscoveryError:
        print(f"DiscoveryError :{DiscoveryError}")
if(cmd.lower()=="g-kill"):

    try:
        _id = int(sys.argv[2])
        registrar = UDPRegistryClient(port=REGISTRY_PORT)
        list_of_servers = registrar.discover("RA")
        targetport=0
        newleaderport=0
        isLeader=False
        portlist=[]
        for server in list_of_servers:
            conn = rpyc.connect(server[0],server[1])
            result = conn.root.detail()
            if(result[0]==_id):
                targetport=server[1]
                if(result[2]==server[1]):
                    isLeader=True
            else:
                portlist.append(server[1])
        print(f'{targetport} {isLeader}')
        if(targetport>0):
            ip=list_of_servers[0][0]
            if(isLeader):
                newleaderport=random.choice(portlist)
                for port in portlist:
                    conn = rpyc.connect(ip,port)
                    conn.root.setLeader(newleaderport)
            registrar.unregister(targetport)
            conn = rpyc.connect(ip,targetport)
            try:
                conn.root.kill()
            except EOFError:
                print(f'{_id} Connection Closed')

    except DiscoveryError:
        print(f"DiscoveryError :{DiscoveryError}")
    except ValueError:
        print(f"Invalid ID")
if(cmd.lower()=="g-add"):
    try:
        _num = int(sys.argv[2])
        registrar = UDPRegistryClient(port=REGISTRY_PORT)
        list_of_servers = registrar.discover("RA")
        node = list_of_servers[0]
        conn = rpyc.connect(node[0],node[1])
        conn.root.newNode(_num)
    except DiscoveryError:
        print(f"DiscoveryError :{DiscoveryError}")
    except ValueError:
        print(f"Invalid ID")
