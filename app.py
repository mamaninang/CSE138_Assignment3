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

app.secret_key = "LandB4Time"

key_value_store = {}
socketAddress = os.getenv('SOCKET_ADDRESS')
myIP, port = os.getenv('SOCKET_ADDRESS').split(':')[0], os.getenv('SOCKET_ADDRESS').split(':')[1]
vectorClock = {"10.10.0.2": 0, "10.10.0.3":0, "10.10.0.4":0}
requestQueue = {}

@app.route('/key-value-store/<key>', methods=['GET','PUT','DELETE'])
def kvs(key):
    defaultlInternalIP = "127.0.0.1"

    #ping other replicas

    if request.method == 'GET':
        return make_response('{"doesExist":true,"message":"Retrieved successfully","value":"%s"}' % key_value_store[key], 200) \
            if key in key_value_store else make_response('{"doesExist":false,"error":"Key does not exist","message":"Error in GET"}', 404)

    if request.method == 'PUT':
        causal = request.json["causal-metadata"]

        #if no causal metadata, then complete request right away
        if causal == "" or vectorClock[myIP] == causal[myIP]:
            #get view of active replicas then update vectorClock

            key_value_store[key] = request.json
            
            #takes max value of each element and adds 1 to own position
            if causal != "":
                for ip in vectorClock:
                    if causal[ip] > vectorClock[ip]:
                        vectorClock[ip] = causal[ip]
            vectorClock[myIP] += 1
            

            #if request is from another replica, check if there's any requests on hold to execute
            if defaultlInternalIP not in request.url_root or requestQueue != {}:
                response = checkRequestQueue()

                return  response if response is not None \
                    else make_response('{"message":"Added successfully", "causal-metadata": "%s"}' % (json.dumps(vectorClock)), 201) if response is None \

            #call broadcast method
            return make_response('{"message":"Added successfully", "causal-metadata": "%s"}' % json.dumps(vectorClock), 201)

        #if metadata != vectorClock, do the following
        else:

            requestQueue[key] = request.json
            return make_response('{"message": "Request placed in a queue", "request": "%s"}' % json.dumps(requestQueue[key]), 200)

            #return make_response('{"message":"Added successfully", "causal-metadata": "%s"}' % json.dumps(vectorClock), 201)

            
    if request.method == 'DELETE':
        if (key in key_value_store.keys()):
            key_value_store.pop(key)
            return make_response('{"doesExist":true,"message":"Deleted successfully"}', 200)
        else: 
            return make_response('{"doesExist":false,"error":"Key does not exist","message":"Error in DELETE"}', 404)
    
def checkRequestQueue():
    headers = {"Content-Type": "application/json"}

    if request.method == 'PUT':
        #reqBefore = requestQueue
        for key in requestQueue:

            if requestQueue[key]["causal-metadata"] == vectorClock:
                key_value_store[key] = req
                vectorClock[myIP] += 1
                #url = "http://" + request.host +":8085"
                del requestQueue[key]
                time.sleep(2)
        
        #reqAfter = requestQueue
        return '{"message":"in checkRequestQueue", "vector":"%s"}' % (json.dumps(vectorClock))


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8085)