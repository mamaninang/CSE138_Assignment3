<<<<<<< HEAD
####################
# Course: CSE138
# Date: Spring 2020
# Author: Alan Vasilkovsky, Mariah Maninang, Bradley Gallardo, Omar Quinones
# Assignment: 3
# Description: 
###################

from flask import Flask, request, make_response, jsonify, Response
import os
import requests
import json
app = Flask(__name__)

key_value_store = {}
myIP = os.getenv('SOCKET_ADDRESS')
vectorClock = {"10.10.0.2:8085": 0, "10.10.0.3:8085":0, "10.10.0.4:8085":0}
requestQueue = {}

viewVar = os.getenv('VIEW')
view = viewVar.split(',')

def wakeup():

    global key_value_store
    for ip in [i for i in view if i != myIP]:
        try:
            other_kvs = requests.get('http://%s/wake' % ip, timeout=1).json()

            if key_value_store != other_kvs:
            
                for key in other_kvs:

                    if key not in key_value_store:

                        key_value_store[key] = other_kvs[key]
                        key_value_store[key]["causal-metadata"][myIP] += 1
                        vectorClock[myIP] += 1

                    elif key in key_value_store and key_value_store[key]["causal-metadata"][myIP] < other_kvs[key]["causal-metadata"][ip]:

                        key_value_store[key] = other_kvs[key]
                        key_value_store[key]["causal-metadata"][myIP] += 1
                        vectorClock[myIP] += 1

                    # update_other_replica_vc()

=======
from flask import Flask, request, make_response, jsonify, redirect, Response, flash
import os
import requests
import json, time
import sys
app = Flask(__name__)

socket_address = os.getenv('SOCKET_ADDRESS')
#socket_address = "10.10.0.2:8085"
myIP = socket_address

viewVar = os.getenv('VIEW')
#viewVar = "10.10.0.2:8085,10.10.0.3:8085,10.10.0.4:8085"
view = viewVar.split(',')

key_value_store = {}

vectorClock = {"10.10.0.2:8085": 0, "10.10.0.3:8085":0, "10.10.0.4:8085":0}
requestQueue = {}

def wakeup():
    global key_value_store
    for ip in [i for i in view if i is not socket_address]:
        try:
            key_value_store = requests.get('http://%s/wake' % ip, timeout=1).json()
>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa
            return
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            continue

<<<<<<< HEAD
=======
wakeup()

print('done')

@app.route('/test')
def test():
    return request.remote_addr, 200

>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa
@app.route('/key-value-store-view', methods=['GET', 'PUT', 'DELETE'])
def view_operations():
    global view

<<<<<<< HEAD

    if request.method == 'GET':
        broadcast(request)

        return make_response('{"message":"View retrieved successfully","view":%s}' % json.dumps(view), 200)


    elif request.method == 'PUT':
        old_view = view
=======
    if request.method == 'GET':
        old_view = view
        sender = request.remote_addr + ":8085"
        broadcast(request)

        return make_response('{"message":"View retrieved succesfully","view":%s}' % json.dumps(view), 200)


    elif request.method == 'PUT':
>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa
        sender = request.remote_addr + ":8085"

        address_to_be_added = request.json['socket-address']

<<<<<<< HEAD
        if sender not in old_view:
            send_missed_requests(sender)

=======
>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa
        if address_to_be_added in view:
            return {"error":"Socket address already exists in the view","message":"Error in PUT"}, 404

        else:
            
            view.append(address_to_be_added)

            if request.remote_addr not in [ip.split(':')[0] for ip in view]:
                broadcast(request)

            return {"message":"Replica added successfully to the view"}, 201

            
    elif request.method == 'DELETE':
        address_to_delete = request.json['socket-address']

        if address_to_delete not in view:
            return {"error":"Socket address does not exist in the view","message":"Error in DELETE"}, 404

        else:
            view.remove(address_to_delete)

            if request.remote_addr not in [ip.split(':')[0] for ip in view]:
                broadcast(request)

            return {"message":"Replica deleted successfully from the view"}, 200

def broadcast(request):
    global view

<<<<<<< HEAD
    broadcast_range = [ip for ip in vectorClock if ip != myIP] 
=======
    broadcast_range = [ip for ip in vectorClock if ip != socket_address] 
>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa
    print("broadcast range: {}".format(broadcast_range))

    for ip in broadcast_range:
        url = 'http://{}:8085{}'.format(ip, request.path)
        print(url)

    if request.method == 'GET':
        old_view = view

        for ip in broadcast_range:
            try:
                res = requests.get('http://{}/status'.format(ip), headers = request.headers, timeout=1).json()
                if res is not None and ip not in view:
                    view.append(ip)
                    # send_missed_requests()

            except requests.exceptions.Timeout:
                if ip in view:
                    view.remove(ip)
                    print(view)


        if old_view != view:
            
            #if ip is not in view and is not own ip, delete address
            addresses_to_delete = [ip for ip in broadcast_range if ip not in view]

            #broadcast range is all addresses in current view except own ip
            del_broadcast_range = [ip for ip in broadcast_range if ip in view]

            #delete inactive replicas from other replicas' views
            for replica in del_broadcast_range:
                for ip in addresses_to_delete:
                    requests.delete('http://{}/key-value-store-view'.format(replica), json={"socket-address": ip})

            #put additional active replicas to other replicas' views
<<<<<<< HEAD
            put_broadcast_range = [ip for ip in view if ip != myIP]
            for replica in put_broadcast_range:
                for ip in view:
                    res = requests.put('http://{}/key-value-store-view'.format(replica), json={"socket-address": ip})
                    return res
=======
            put_broadcast_range = [ip for ip in view if ip != socket_address]
            for replica in put_broadcast_range:
                for ip in view:
                    res = requests.put('http://{}/key-value-store-view'.format(replica), json={"socket-address": ip})
>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa
            


    elif request.method == 'PUT' and request.json is not None:
        for ip in broadcast_range:
            try:
                requests.put('http://{}{}'.format(ip, request.path), json=request.json, timeout=1)
            except requests.exceptions.Timeout:
                view.remove(ip)
                print(view)
                
<<<<<<< HEAD
                del_broadcast_range = [ip for ip in view if ip != myIP]
=======
                del_broadcast_range = [ip for ip in view if ip != socket_address]
>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa
                for replica in del_broadcast_range:
                    try:
                        requests.delete('http://{}/key-value-store-view'.format(replica), json={"socket-address": ip}, timeout=1)
                    except requests.exceptions.Timeout:
                        continue
                continue
        # return

    elif request.method == 'DELETE' and request.json is not None:
        for ip in broadcast_range:
            try:
                requests.delete('http://{}{}'.format(ip, request.path), json=request.json, timeout=1)
            except requests.exceptions.Timeout:
                view.remove(ip)
                print(view)
                
                del_broadcast_range = [ip for ip in view if ip != socket_address]
                for replica in del_broadcast_range:
                    try:
                        requests.delete('http://{}/key-value-store-view'.format(replica), json={"socket-address": ip}, timeout=1)
                    except requests.exceptions.Timeout:
                        continue
                continue
<<<<<<< HEAD
=======
        # return
>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa

@app.route('/key-value-store/<key>', methods=['GET','PUT','DELETE'])
def kvs(key):
    global view
<<<<<<< HEAD
=======
    global key_value_store

>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa

    sender = request.remote_addr + ":8085"

    if sender not in vectorClock:
        res = requests.get('http://{}/key-value-store-view'.format(myIP), headers = request.headers).json()
        view = res["view"]
<<<<<<< HEAD

        wakeup()


    if request.method == 'GET':
    
        if key_value_store[key] is None or key not in key_value_store:
            return make_response('{"doesExist":false,"error":"Key does not exist","message":"Error in GET"}', 404)
        else:
            causal = json.dumps(key_value_store[key]["causal-metadata"])
            value = key_value_store[key]["value"]

            return make_response('{"doesExist":true,"message":"Retrieved successfully","causal-metadata":%s,"value":"%s"}' % (causal, value) , 200)


    if request.method == 'PUT':

        causal = request.json["causal-metadata"]
        value = request.json

        keyAlreadyExists = True if key in key_value_store else False
=======
        

    if request.method == 'GET':

        if key not in key_value_store or key_value_store[key] is None:
            return make_response('{"message":"Key does not exist"}', 404)

        else:
            causal = json.dumps(key_value_store[key]["causal-metadata"])
            value = key_value_store[key]["value"]
            return make_response('{"message":"Retrieved successfully", "causal-metadata":%s, "value":"%s"}' % (causal, value), 200)
  


    if request.method == 'PUT':
        causal = request.json["causal-metadata"]
        value = request.json

        if key in key_value_store:
            successMsg, statusCode = "Updated successfully", 200
        else:
            successMsg, statusCode = "Added successfully", 201
>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa

        if causal == "":

            key_value_store[key] = request.json
            vectorClock[myIP] += 1

<<<<<<< HEAD
=======
            #check requests on hold
            if requestQueue != {}:
                done = False
                while done is not True:
                    done = checkRequestQueue()

            kvs_broadcast(sender, key, value, request.method)

            key_value_store[key]["causal-metadata"] = vectorClock
            vectorClockJson = json.dumps(vectorClock)
            return make_response('{"message":"%s", "causal-metadata": %s}' % (successMsg, vectorClockJson), statusCode)

>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa
        else:

            if causal[myIP] > vectorClock[myIP]:
                requestQueue[key] = request.json
                requestQueue[key]["method"] = request.method
<<<<<<< HEAD
                return make_response('{"message": "Request placed in a queue", "request": %s}' % json.dumps(requestQueue[key]), 200)
=======
                requestQueue[key]["sender"] = request.remote_addr + ":8085"
                return ('{"message": "Request placed in a queue", "request": %s}' % json.dumps(requestQueue[key]), 200)
>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa

            else:

                key_value_store[key] = request.json

                #takes max value of each element if sender is a replica and adds 1 to own position
                if sender not in vectorClock:
                    vectorClock[myIP] += 1
                else:
                    takeMaxElement(causal)
                    vectorClock[myIP] += 1

<<<<<<< HEAD
        key_value_store[key]["causal-metadata"] = vectorClock
        kvs_broadcast(sender, key, value, request.method)
        update_other_replica_vc(sender)

        #check requests on hold
        if requestQueue != {}:
            done = False
            while done is not True:
                done = checkRequestQueue()

        vectorClockJson = json.dumps(vectorClock)
        if keyAlreadyExists ==  True:
            return make_response('{"message":"Updated successfully", "causal-metadata": %s}' % vectorClockJson, 200)
        else:
            return make_response('{"message":"Added successfully", "causal-metadata": %s}' % vectorClockJson, 201)

            
    if request.method == 'DELETE':

        causal = request.json["causal-metadata"]
        sender = request.remote_addr + ":8085"
=======
                #check requests on hold
                if requestQueue != {}:
                    done = False
                    while done is not True:
                        done = checkRequestQueue()

                kvs_broadcast(sender, key, value, request.method)

                vectorClockJson = json.dumps(vectorClock)
                return make_response('{"message":"%s", "causal-metadata": %s}' % (successMsg, vectorClockJson), statusCode)

            
    if request.method == 'DELETE':
        causal = request.json["causal-metadata"]
>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa
        value = request.json

        if key not in key_value_store:
            return make_response('{"doesExist":false,"error":"Key does not exist","message":"Error in DELETE"}', 404)

        elif causal[myIP] > vectorClock[myIP]:
            requestQueue[key] = request.json
            requestQueue[key]["method"] = request.method
<<<<<<< HEAD
=======
            requestQueue[key]["sender"] = request.remote_addr + ":8085"
>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa
            return make_response('{"message": "Request placed in a queue", "request": %s}' % json.dumps(requestQueue[key]), 200)

        elif key_value_store[key] is None:
            return make_response('{"doesExist":false,"error":"Key already deleted","message":"Error in DELETE"}', 404)

        else:
            key_value_store[key] = None

            #takes max value of each element if sender is a replica and adds 1 to own position
            if sender not in vectorClock:
                vectorClock[myIP] += 1
            else:
                takeMaxElement(causal)
                vectorClock[myIP] += 1

<<<<<<< HEAD
            key_value_store[key]["causal-metadata"] = vectorClock
            kvs_broadcast(sender, key, value, request.method)
            update_other_replica_vc(sender)


=======
>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa
            #check requests on hold
            if requestQueue != {}:
                done = False
                while done is not True:
                    done = checkRequestQueue()

<<<<<<< HEAD
            vectorClockJson = json.dumps(vectorClock)
            return make_response('{"message":"Deleted successfully", "causal-metadata":%s}' % vectorClockJson, 200) 
=======
            kvs_broadcast(sender, key, value, request.method)

            vectorClockJson = json.dumps(vectorClock)
            return make_response('{"doesExist":true,"message":"Deleted successfully", "causal-metadata":"%s"}' % vectorClockJson, 200) 
>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa

    
def checkRequestQueue():

    for key in requestQueue:

        causal = requestQueue[key]["causal-metadata"]
        if causal[myIP] <= vectorClock[myIP]:

<<<<<<< HEAD
            if requestQueue[key]["method"] == 'PUT':
                key_value_store[key] = requestQueue[key]
                method = 'PUT'

            elif requestQueue[key]["method"] == 'DELETE':
                key_value_store[key] = None
                method = 'DELETE'

            sender = request.remote_addr + ":8085"
=======
            sender = requestQueue[key].pop("sender")
            method = requestQueue[key].pop("method")

            if method == 'PUT':
                key_value_store[key] = requestQueue[key]

            elif method == 'DELETE':
                key_value_store[key] = None

>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa
            value = key_value_store[key]

            del requestQueue[key]
            vectorClock[myIP] += 1
<<<<<<< HEAD
=======

>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa
            kvs_broadcast(sender, key, value, method)

            return False
        
    return True
    
<<<<<<< HEAD
#work on this more??
=======

>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa
def takeMaxElement(causal):
    for ip in vectorClock:
        if causal[ip] > vectorClock[ip]:
            vectorClock[ip] = causal[ip]
<<<<<<< HEAD
        

def kvs_broadcast(sender, key, value, method):
    
    #if sender is not a replica, broadcast
    if sender not in vectorClock:
        for ip in view:
            if ip != myIP:

                address = "http://" + ip + "/key-value-store/" + key
                value["causal-metadata"] = vectorClock

                if method == 'PUT':
                    response = requests.put(address, headers=request.headers, json=value)

                elif method == 'DELETE':
                    response = requests.delete(address, headers=request.headers, json=value)

                responseJson = response.json()
                newVector = responseJson["causal-metadata"]
                takeMaxElement(newVector)


def update_other_replica_vc(sender):
    if sender not in vectorClock:
        ip_to_get_vc = [ip for ip in view if ip != myIP]

        for ip in ip_to_get_vc:
            requests.put('http://{}/send-vc'.format(ip), headers = request.headers, json = {"vector-clock": vectorClock})
    return


@app.route('/send-vc', methods = ['PUT'])
def send_vc():
    other_vc = request.json["vector-clock"]
    takeMaxElement(other_vc)
    return

@app.route('/status', methods = ['GET'])
def status():
    return make_response('{"status": "alive"}')

=======


def kvs_broadcast(sender, key, value, method):
    global view

    #if sender is not a replica, broadcast
    if sender not in vectorClock:
        broadcast_range = [ip for ip in view if ip != socket_address] 
        for ip in broadcast_range:

            address = "http://" + ip + "/key-value-store/" + key
            value["causal-metadata"] = key_value_store[key]["causal-metadata"]

            if method == 'PUT':
                response = requests.put(address, headers=request.headers, json=value)

            elif method == 'DELETE':
                response = requests.delete(address, headers=request.headers, json=value)

            responseJson = response.json()
            newVector = responseJson["causal-metadata"]
            takeMaxElement(newVector)



@app.route('/status', methods = ['GET'])
def status():
    global view

    return make_response('{"status": "alive"}')

# @app.route('/get-vc', methods = ['GET'])
#     return {"vector-clock": json.dumps(vectorClock)}

# @app.route('/update-kvs', methods = ['PUT'])
# def update_kvs():
#     global key_value_store

#     key_value_store = request.json["key-value-store"]

>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa
@app.route('/wake', methods=['GET'])
def wake():
    return key_value_store, 200

if __name__ == '__main__':
<<<<<<< HEAD
    app.run(debug=True, host='0.0.0.0', port=8085)
=======
    app.run(debug=True, host='0.0.0.0', port=8085)
    
    
>>>>>>> 18d454be469f86cde17602b4168dbab7af82d3fa
