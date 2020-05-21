from flask import Flask, request, make_response, jsonify, redirect, Response, flash
import os
import requests
import json, time
import sys
app = Flask(__name__)

#socket_address = os.getenv('SOCKET_ADDRESS')
socket_address = "10.10.0.2:8085"
myIP = socket_address.split(':')[0]

#viewVar = os.getenv('VIEW')
viewVar = "10.10.0.2:8085,10.10.0.3:8085,10.10.0.4:8085"
view = viewVar.split(',')

key_value_store = {}

vectorClock = {"10.10.0.2": 0, "10.10.0.3":0, "10.10.0.4":0}
requestQueue = {}

@app.route('/test')
def test():
    return request.remote_addr, 200

@app.route('/key-value-store/<key>', methods=['GET','PUT','DELETE'])
def kvs(key):
    sender = request.remote_addr

    if request.method == 'GET':
        if key not in key_value_store or key_value_store[key] is None:
            return make_response('{"message":"Key does not exist"}', 404)
        else:
            return make_response('"message":"Retrieved successfully", "casual-metadata":"%s", "value":"%s"}' % (key_value_store[key]["causal-metadata"], key_value_store[key]["value"]), 200)

    if request.method == 'PUT':
        causal = request.json["causal-metadata"]

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

            broadcast(request)

            vectorClockJson = json.dumps(vectorClock)
            return make_response('{"message":"%s", "causal-metadata": %s}' % (successMsg, vectorClockJson), statusCode)

        else:

            if causal[myIP] > vectorClock[myIP]:
                requestQueue[key] = request.json
                requestQueue[key]["method"] = request.method
                return ('{"message": "Request placed in a queue", "request": %s}' % json.dumps(requestQueue[key]), 200)

            else:

                key_value_store[key] = request.json

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

                broadcast(request)

                vectorClockJson = json.dumps(vectorClock)
                return make_response('{"message":"%s", "causal-metadata": %s}' % (successMsg, vectorClockJson), statusCode)

            
    if request.method == 'DELETE':
        causal = request.json["causal-metadata"]

        if key not in key_value_store:
            return make_response('{"doesExist":false,"error":"Key does not exist","message":"Error in DELETE"}', 404)

        elif causal[myIP] > vectorClock[myIP]:
            requestQueue[key] = request.json
            requestQueue[key]["method"] = request.method
            return make_response('{"message": "Request placed in a queue", "request": %s}' % json.dumps(requestQueue[key]), 200)

        elif key_value_store[key] is None:
            return make_response('{"doesExist":false,"error":"Key already deleted","message":"Error in DELETE"}', 404)

        else:
            key_value_store[key] = None

            #takes max value of each element and adds 1 to own position
            if causal != "":    
                takeMaxElement(causal)
            vectorClock[myIP] += 1

            #check requests on hold
            if requestQueue != {}:
                done = False
                while done is not True:
                    done = checkRequestQueue()

            broadcast(request)

            vectorClockJson = json.dumps(vectorClock)
            return make_response('{"doesExist":true,"message":"Deleted successfully", "causal-metadata":"%s"}' % vectorClockJson, 200) 

    
def checkRequestQueue():

    for key in requestQueue:

        causal = requestQueue[key]["causal-metadata"]
        if causal[myIP] <= vectorClock[myIP]:

            if requestQueue[key]["method"] == 'PUT':
                key_value_store[key] = requestQueue[key]

            elif requestQueue[key]["method"] == 'DELETE':
                key_value_store[key] = None

            del requestQueue[key]
            vectorClock[myIP] += 1
            return False
        
    return True
    

def takeMaxElement(causal):
    for ip in vectorClock:
        if causal[ip] > vectorClock[ip]:
            vectorClock[ip] = causal[ip]

@app.route('/key-value-store-view', methods=['GET', 'PUT', 'DELETE'])
def view_operations():
    if request.method == 'GET':
        return {"message":"View retrieved succesfully","view":view}, 200

    elif request.method == 'PUT':
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

    broadcast_range = [ip for ip in view if ip != socket_address] 
    print("broadcast range: {}".format(broadcast_range))

    for ip in broadcast_range:
        url = 'http://{}:8085{}'.format(ip, request.path)
        print(url)

    if request.method == 'PUT' and request.json is not None:
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
        return

    if request.method == 'DELETE' and request.json is not None:
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
        return

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8085)