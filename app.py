####################
# Course: CSE138
# Date: Spring 2020
# Author: Alan Vasilkovsky, Mariah Maninang, Bradley Gallardo, Omar Quinones
# Assignment: 2
# Description: 
###################

from flask import Flask, request, make_response, jsonify, redirect, Response, flash
import os
import requests
import json, time
app = Flask(__name__)

key_value_store = {}
myIP = os.getenv('SOCKET_ADDRESS').split(':')[0]
vectorClock = {"10.10.0.2": 0, "10.10.0.3":0, "10.10.0.4":0}
requestQueue = {}

@app.route('/key-value-store/<key>', methods=['GET','PUT','DELETE'])
def kvs(key):
    defaultlInternalIP = "127.0.0.1"

    ##get view of active replicas then update vectorClock here

    if request.method == 'GET':
        return make_response('{"doesExist":true,"message":"Retrieved successfully","value":"%s"}' % key_value_store[key], 200) \
            if key in key_value_store else make_response('{"doesExist":false,"error":"Key does not exist","message":"Error in GET"}', 404)

    if request.method == 'PUT':
        causal = request.json["causal-metadata"]

        if causal == "" or causal[myIP] <= vectorClock[myIP]:

            successMsg = "Updated successfully" if key in key_value_store else "Added successfully"
            key_value_store[key] = request.json
            
            #takes max value of each element and adds 1 to own position
            if causal != "":    
                compareVC(causal)
            vectorClock[myIP] += 1
            
            #check requests on hold
            response = True
            if requestQueue != {}:
                while response is not None:
                    response = checkRequestQueue()

            #if request is from client, broadcast the request here

            vectorClockJson = json.dumps(vectorClock)
            return make_response('{"message":"%s", "causal-metadata": %s}' % (successMsg, vectorClockJson), 201)

        else:

            requestQueue[key] = request.json
            requestQueue[key]["method"] = request.method
            return ('{"message": "Request placed in a queue", "request": %s}' % json.dumps(requestQueue[key]), 200)

            
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
                compareVC(causal)
            vectorClock[myIP] += 1

            #check requests on hold
            response = True
            if requestQueue != {}:
                while response is not None:
                    response = checkRequestQueue()
            
            #if request is from client, broadcast the request here

            vectorClockJson = json.dumps(vectorClock)
            return make_response('{"doesExist":true,"message":"Deleted successfully", "causal-metadata":"%s"}' % vectorClockJson, 200) 

    
def checkRequestQueue():
    headers = {"Content-Type": "application/json"}

    for key in requestQueue:

        causal = requestQueue[key]["causal-metadata"]
        if causal[myIP] <= vectorClock[myIP]:

            if requestQueue[key]["method"] == 'PUT':
                key_value_store[key] = requestQueue[key]

            elif requestQueue[key]["method"] == 'DELETE':
                key_value_store[key] = None

            del requestQueue[key]
            vectorClock[myIP] += 1
            return 'Not done checking request queue'
    
    return None
    

def compareVC(causal):
    for ip in vectorClock:
        if causal[ip] > vectorClock[ip]:
            vectorClock[ip] = causal[ip]

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8085)