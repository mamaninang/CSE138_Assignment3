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
            return
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            continue

wakeup()

print('done')

@app.route('/test')
def test():
    return request.remote_addr, 200

@app.route('/key-value-store-view', methods=['GET', 'PUT', 'DELETE'])
def view_operations():
    global view

    if request.method == 'GET':
        old_view = view
        sender = request.remote_addr + ":8085"
        broadcast(request)

        return make_response('{"message":"View retrieved succesfully","view":%s}' % json.dumps(view), 200)


    elif request.method == 'PUT':
        sender = request.remote_addr + ":8085"

        address_to_be_added = request.json['socket-address']

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

    broadcast_range = [ip for ip in vectorClock if ip != socket_address] 
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
            put_broadcast_range = [ip for ip in view if ip != socket_address]
            for replica in put_broadcast_range:
                for ip in view:
                    res = requests.put('http://{}/key-value-store-view'.format(replica), json={"socket-address": ip})
            


    elif request.method == 'PUT' and request.json is not None:
        for ip in broadcast_range:
            try:
                requests.put('http://{}{}'.format(ip, request.path), json=request.json, timeout=1)
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
        # return

@app.route('/key-value-store/<key>', methods=['GET','PUT','DELETE'])
def kvs(key):
    global view
    global key_value_store


    sender = request.remote_addr + ":8085"

    if sender not in vectorClock:
        res = requests.get('http://{}/key-value-store-view'.format(myIP), headers = request.headers).json()
        view = res["view"]
        

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

        if causal == "":

            key_value_store[key] = request.json
            vectorClock[myIP] += 1

            #check requests on hold
            if requestQueue != {}:
                done = False
                while done is not True:
                    done = checkRequestQueue()

            kvs_broadcast(sender, key, value, request.method)

            key_value_store[key]["causal-metadata"] = vectorClock
            vectorClockJson = json.dumps(vectorClock)
            return make_response('{"message":"%s", "causal-metadata": %s}' % (successMsg, vectorClockJson), statusCode)

        else:

            if causal[myIP] > vectorClock[myIP]:
                requestQueue[key] = request.json
                requestQueue[key]["method"] = request.method
                requestQueue[key]["sender"] = request.remote_addr + ":8085"
                return ('{"message": "Request placed in a queue", "request": %s}' % json.dumps(requestQueue[key]), 200)

            else:

                key_value_store[key] = request.json

                #takes max value of each element if sender is a replica and adds 1 to own position
                if sender not in vectorClock:
                    vectorClock[myIP] += 1
                else:
                    takeMaxElement(causal)
                    vectorClock[myIP] += 1

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
        value = request.json

        if key not in key_value_store:
            return make_response('{"doesExist":false,"error":"Key does not exist","message":"Error in DELETE"}', 404)

        elif causal[myIP] > vectorClock[myIP]:
            requestQueue[key] = request.json
            requestQueue[key]["method"] = request.method
            requestQueue[key]["sender"] = request.remote_addr + ":8085"
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

            #check requests on hold
            if requestQueue != {}:
                done = False
                while done is not True:
                    done = checkRequestQueue()

            kvs_broadcast(sender, key, value, request.method)

            vectorClockJson = json.dumps(vectorClock)
            return make_response('{"doesExist":true,"message":"Deleted successfully", "causal-metadata":"%s"}' % vectorClockJson, 200) 

    
def checkRequestQueue():

    for key in requestQueue:

        causal = requestQueue[key]["causal-metadata"]
        if causal[myIP] <= vectorClock[myIP]:

            sender = requestQueue[key].pop("sender")
            method = requestQueue[key].pop("method")

            if method == 'PUT':
                key_value_store[key] = requestQueue[key]

            elif method == 'DELETE':
                key_value_store[key] = None

            value = key_value_store[key]

            del requestQueue[key]
            vectorClock[myIP] += 1

            kvs_broadcast(sender, key, value, method)

            return False
        
    return True
    

def takeMaxElement(causal):
    for ip in vectorClock:
        if causal[ip] > vectorClock[ip]:
            vectorClock[ip] = causal[ip]


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

@app.route('/wake', methods=['GET'])
def wake():
    return key_value_store, 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8085)
    
    