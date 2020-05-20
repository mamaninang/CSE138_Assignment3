####################
# Course: CSE138
# Date: Spring 2020
# Author: Alan Vasilkovsky, Mariah Maninang, Bradley Gallardo, Omar Quinones
# Assignment: 2
# Description: 
###################

from flask import Flask, request, make_response, jsonify, Response
import os
import requests
import json
app = Flask(__name__)

key_value_store = {}
myIP = os.getenv('SOCKET_ADDRESS').split(':')[0]
vectorClock = {"10.10.0.2": 0, "10.10.0.3":0, "10.10.0.4":0}
requestQueue = {}

@app.route('/key-value-store/<key>', methods=['GET','PUT','DELETE'])
def kvs(key):
    #view = requests.get("http://" + myIP + ":8085/key-value-store-view/", headers=request.headers)
    #def updateKVS(view)
    #for ip in vectorClock:
    #   if ip not in view:
    #       vectorClock[ip] = None
    #   elif ip in view and vectorClock[ip] == None:
    #       vectorClock[ip] = 0
    ##get view of active replicas then update vectorClock here

    causal = request.json["causal-metadata"]
    sender = request.remote_addr
    value = request.json

    if request.method == 'GET':
        
        if key_value_store[key] is None or key not in key_value_store:
            return make_response('{"doesExist":false,"error":"Key does not exist","message":"Error in GET"}', 404)
        else:
            return make_response('{"doesExist":true,"message":"Retrieved successfully","value":"%s"}' % key_value_store[key]["value"], 200)


    if request.method == 'PUT':

        keyAlreadyExists = True if key in key_value_store else False

        if causal == "":

            key_value_store[key] = request.json
            vectorClock[myIP] += 1

        else:

            if causal[myIP] > vectorClock[myIP]:
                requestQueue[key] = request.json
                requestQueue[key]["method"] = request.method
                return make_response('{"message": "Request placed in a queue", "request": %s}' % json.dumps(requestQueue[key]), 200)

            else:

                key_value_store[key] = request.json

                if sender not in vectorClock:
                    vectorClock[myIP] += 1
                else:
                    takeMaxElement(causal)
                    vectorClock[myIP] += 1

        broadcast(sender, key, value, request.method)

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
            if sender not in vectorClock:
                vectorClock[myIP] += 1
            else:
                takeMaxElement(causal)
                vectorClock[myIP] += 1
            
            broadcast(sender, key, value, request.method)

            #check requests on hold
            if requestQueue != {}:
                done = False
                while done is not True:
                    done = checkRequestQueue()

            vectorClockJson = json.dumps(vectorClock)
            return make_response('{"message":"Deleted successfully", "causal-metadata":%s}' % vectorClockJson, 200) 

    
def checkRequestQueue():

    for key in requestQueue:

        causal = requestQueue[key]["causal-metadata"]
        if causal[myIP] <= vectorClock[myIP]:

            if requestQueue[key]["method"] == 'PUT':
                key_value_store[key] = requestQueue[key]
                method = 'PUT'

            elif requestQueue[key]["method"] == 'DELETE':
                key_value_store[key] = None
                method = 'DELETE'

            sender = request.method
            value = key_value_store[key]

            del requestQueue[key]
            vectorClock[myIP] += 1
            broadcast(sender, key, value, method)

            return False
        
    return True
    

def takeMaxElement(causal):
    for ip in vectorClock:
        if causal[ip] > vectorClock[ip]:
            vectorClock[ip] = causal[ip]
        

def broadcast(sender, key, value, method):
    

    #if sender is not a replica, broadcast
    if sender not in vectorClock:
        for ip in vectorClock:
            if ip != myIP:

                address = "http://" + ip + ":8085/key-value-store/" + key
                value["causal-metadata"] = vectorClock

                if method == 'PUT':
                    response = requests.put(address, headers=request.headers, json=value)

                elif method == 'DELETE':
                    response = requests.delete(address, headers=request.headers, json=value)

                responseJson = response.json()
                newVector = responseJson["causal-metadata"]
                takeMaxElement(newVector)



if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8085)