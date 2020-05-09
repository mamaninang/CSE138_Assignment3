####################
# Course: CSE138
# Date: Spring 2020
# Author: Alan Vasilkovsky, Mariah Maninang, Bradley Gallardo, Omar Quinones
# Assignment: 2
# Description: 
###################

from flask import Flask, request, make_response
import os
import requests
app = Flask(__name__)

key_value_store = {}
socketAddress = os.getenviron("SOCKET_ADDRESS")     
vectorClock = {"10.10.0.2": 0, "10.10.0.3":0, "10.10.0.4":0}

@app.route('/key-value-store/<key>', methods=['GET','PUT','DELETE'])
def kvs(key):

    #ping other replicas
    vectorClockJson = json.loads(vectorClock)
    socketAddress = 

    if request.method == 'GET':
        return make_response({"doesExist":true,"message":"Retrieved successfully","value":"%s"} % key_value_store[key], 200) \
            if key in key_value_store else make_response('{"doesExist":false,"error":"Key does not exist","message":"Error in GET"}', 404)

    if request.method == 'PUT':
        causalMetadata = request.json["causal-metadata"]


        if causalMetadata is None:
            kvs[key] = request.json
            vectorClock[socketAddress] += 1
            return make_response('{"message":"Added successfully", "causal-metadata": "{vectorClockJson}"}', 201)
        else:
            if causalMetadata in requestQueue[key]:
                kvs[key] = requestQueue[key]
                del requestQueue[key]
                vectorClock[socketAddress] += 1
                return make_response('{"message":"Added successfully", "causal-metadata": "<V2>"}', 201)
            else:
                requestQueue[key] = request.json
                #do nothing, keep request in requestQueue

        #after completing put request, check if any request in requestQueue causally depend on current request
        if causalMetadata in requestQueue[key] or key not in kvs:
                kvs[key] = requestQueue[key]
                del requestQueue[key]
                return make_response('{"message":"Added successfully", "causal-metadata": "<V2>"}', 201)    
        #elif key in kvs 

        causalBroadcast()

            
    if request.method == 'DELETE':
        if (key in key_value_store.keys()):
            key_value_store.pop(key)
            return make_response('{"doesExist":true,"message":"Deleted successfully"}', 200)
        else: 
            return make_response('{"doesExist":false,"error":"Key does not exist","message":"Error in DELETE"}', 404)
    
def checkRequestQueue(key, causalMetadata):
    if key in requestQueue and causalMetadata is in requestQueue[key]:


def causalBroadcast():
    if forwarding_address is None:
        #do something
    else:

        new_address = 'http://' + forwarding_address + request.path
        res = None
        timeout = 0.01
        headers = {"Content-Type": "application/json"}


        try:
            if request.method == 'GET':
                res = requests.get(new_address, headers=headers, timeout=timeout)
            
            if request.method == 'PUT':
                res = requests.put(new_address, headers=headers, json=request.json, timeout=timeout)
            
            if request.method == 'DELETE':
                res = requests.delete(new_address, headers=headers, timeout=timeout)

        except requests.exceptions.Timeout:
            return '{"error":"Main instance is down","message":"Error in %s"}' % request.method, 503

        return res.text, res.status_code

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8085)