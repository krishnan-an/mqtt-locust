import random
import time
import sys
import ssl
import datetime
import os
import json
import resource
import paho.mqtt.client as mqtt
from locust import Locust, task, TaskSet, events, User, SequentialTaskSet, constant, HttpUser
import socket
from influxdb import InfluxDBClient
import pytz

hostname = socket.gethostname()
client = InfluxDBClient(host="localhost", port="8086")
client.switch_database('locust_db')

REQUEST_TYPE = 'MQTT'
MESSAGE_TYPE_PUB = 'PUB'
MESSAGE_TYPE_SUB = 'SUB'

RETRY = 5

#ms
PUBLISH_TIMEOUT = 10000
SUBSCRIBE_TIMEOUT = 10000

payload = "TestMessage"
topic = "test1"


def individual_success_handle(request_type, name, response_time, response_length, **kwargs):
    SUCCESS_TEMPLATE = '[{"measurement": "%s","tags": {"hostname":"%s","requestName": "%s","requestType": "%s","status":"%s"' \
                       '},"time":"%s","fields": {"responseTime": %f,"responseLength":%d}' \
                       '}]'
    json_string = SUCCESS_TEMPLATE % (
    "ResponseTable", hostname, name, request_type, "success", datetime.datetime.now(tz=pytz.UTC), float(response_time),
    int(response_length))
    client.write_points(json.loads(json_string), time_precision='ms')



def individual_fail_handle(request_type, name, response_time, response_length, **kwargs):
    FAIL_TEMPLATE = '[{"measurement": "%s","tags": {"hostname":"%s","requestName": "%s","requestType": "%s","status":"%s"' \
                    '},"time":"%s","fields": {"responseTime": %f,"responseLength":%d}' \
                    '}]'
    json_string = FAIL_TEMPLATE % (
    "ResponseTable", hostname, name, request_type, "fail", datetime.datetime.now(tz=pytz.UTC),
    float(response_time), int(response_length))
    client.write_points(json.loads(json_string), time_precision='ms')


events.request_success.add_listener(individual_success_handle)
events.request_success.add_listener(individual_fail_handle)


def time_delta(t1, t2):
    return int((t2 - t1) * 1000)


def fire_locust_failure(**kwargs):
    events.request_failure.fire(**kwargs)


def fire_locust_success(**kwargs):
    events.request_success.fire(**kwargs)


class LocustError(Exception):
    pass


class TimeoutError(ValueError):
    pass


class ConnectError(Exception):
    pass

class DisconnectError(Exception):
    pass


class Message(object):

    def __init__(self, type, qos, topic, payload, start_time, timeout, name):
        self.type = type,
        self.qos = qos,
        self.topic = topic
        self.payload = payload
        self.start_time = start_time
        self.timeout = timeout
        self.name = name

    def timed_out(self, total_time):
        return self.timeout is not None and total_time > self.timeout


class MQTTClient():
    mqtt_bridge_hostname = <MQTTHOST>
    mqtt_bridge_port = <MQTTPORT>

    def __init__(self, *args, **kwargs):
        self.client = mqtt.Client(client_id="Perf_client_"+str(int(time.time())), clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp")
        self.client.on_connect = self.locust_on_connect
        self.client.on_publish = self.locust_on_publish
        self.client.on_subscribe = self.locust_on_subscribe
        self.client.on_disconnect = self.locust_on_disconnect
        self.pubmmap = {}
        self.submmap = {}
        self.defaultQoS = 0
    
    def connect_to_server(self):
        print("Connecting..")
        start_time = time.time()
        try:
            self.client.connect(self.mqtt_bridge_hostname, self.mqtt_bridge_port,keepalive=60)
            self.client.loop_start()
            self.client.user_data_set(start_time)
        except Exception as e:
            total_time = round((time.time() - start_time)*1000, ndigits=3)
            events.request_failure.fire(request_type=REQUEST_TYPE, name='connect_fail', response_time=total_time, exception=e,response_length=0)
        
    def disconnect(self):
        print("Disconnecting..")
        start_time = time.time()
        try:
            self.client.loop_stop() 
            self.client.disconnect()
            self.client.user_data_set(start_time)
        except Exception as e:
            total_time = round((time.time() - start_time)*1000, ndigits=3)
            events.request_failure.fire(request_type="mqtt_disconnect", name='disconnect', response_time=total_time, exception=e)


    #retry is not used at the time since this implementation only supports QoS 0
    def publish(self, topic, payload=None, qos=0, retry=5, name='publish', **kwargs):
        payload = payload+str(time.time())
        print("Publishing Message {} in topic {}".format(payload, topic))
        timeout = kwargs.pop('timeout', 10000)
        start_time = time.time()
        try:
          err, mid = self.client.publish(topic,payload=payload,qos=qos,**kwargs)
          if err:
            fire_locust_failure(
                    request_type=REQUEST_TYPE,
                    name=name,
                    response_time=time_delta(start_time, time.time()),
                    exception=ValueError(err),
                    response_length=0
                )

            # print ("publish: err,mid:["+str(err)+","+str(mid)+"]")
          self.pubmmap[mid] = Message(MESSAGE_TYPE_PUB, qos, topic, payload, start_time, timeout, name)
          #print ("publish: Saved message - mqtt client obj id:["+str(id(self))+"] - pubmmap obj id:["+str(id(self.pubmmap))+"] in dict - mid:["+str(mid)+"] - message object id:["+str(id(self.pubmmap[mid]))+"]")                        
        except Exception as e:
          fire_locust_failure(
                    request_type=REQUEST_TYPE,
                    name=name,
                    response_time=time_delta(start_time, time.time()),
                    exception=e,
                    response_length=0
                )
          print (str(e))

    def subscribe(self, topic, qos=0, retry=5, name='subscribe', timeout=15000):
        print ("Subscribing to topic:["+topic+"]")
        start_time = time.time()
        try:
            err, mid = self.client.subscribe(topic,qos=qos)
            self.submmap[mid] = Message(MESSAGE_TYPE_SUB, qos, topic, "", start_time, timeout, name)
            if err:
              raise ValueError(err)
            #   print ("Subscribed to topic with err:["+str(err)+"]messageId:["+str(mid)+"]")
        except Exception as e:
          total_time = time_delta(start_time, time.time())
          fire_locust_failure(
                  request_type=REQUEST_TYPE,
                  name=name,
                  response_time=total_time,
                  exception=e,
                  response_length=0
          )
        #   print ("Exception when subscribing to topic:["+str(e)+"]")
        

    def locust_on_connect(self, client, userdata,flags, rc):
        print("on connect")
        total_time = time_delta(userdata, time.time())
        self.subscribe(topic, qos=0, retry=5, name='subscribe:'+topic, timeout=SUBSCRIBE_TIMEOUT)
        # print("Connection returned result: "+mqtt.connack_string(rc))        
        fire_locust_success(
            request_type=REQUEST_TYPE,
            name='connect',
            response_time=total_time,
            response_length=0
            )
        
    def locust_on_publish(self, client, userdata, mid):
        print("on Publish")
        end_time = time.time()
        
        if self.defaultQoS == 0:
          #if QoS=0, we reach the callback before the publish() has enough time to update the pubmmap dictionary
            time.sleep(float(0.5))

        message = self.pubmmap.pop(mid, None)        
        #print ("on_publish  - mqtt client obj id:["+str(id(self))+"] - pubmmap obj id:["+str(id(self.pubmmap))+"] - mid:["+str(mid)+"] - message obj id:["+str(id(message))+"]")
        if message is None:
          fire_locust_failure(
                request_type=REQUEST_TYPE,
                name="mqtt_publish_no_message",
                response_time=0,
                exception=ValueError("Published message could not be found"),response_length=0
          )
          return
        
        total_time = time_delta(message.start_time, end_time)
        if message.timed_out(total_time):
          fire_locust_failure(
                request_type=REQUEST_TYPE,
                name=message.name,
                response_time=total_time,
                exception=TimeoutError("publish timed out"),response_length=0
          )
          #print("report publish failure - response_time:["+str(total_time)+"]")
        else:
            fire_locust_success(
                request_type=REQUEST_TYPE,
                name=message.name,
                response_time=total_time,
                response_length=len(message.payload),
            )
            # print("report publish success - response_time:["+str(total_time)+"]")


    def locust_on_subscribe(self, client, userdata, mid, granted_qos):
        print("on Subscribe")
        end_time = time.time()
        message = self.submmap.pop(mid, None)
        if message is None:
            # print ("did not find message for on_subscribe")
            return
        total_time = time_delta(message.start_time, end_time)
        if message.timed_out(total_time):
            fire_locust_failure(
                request_type=REQUEST_TYPE,
                name=message.name,
                response_time=total_time,
                exception=TimeoutError("subscribe timed out"),response_length=0
            )
            # print("report subscribe failure - response_time:["+str(total_time)+"]")
        else:
            fire_locust_success(
                request_type=REQUEST_TYPE,
                name=message.name,
                response_time=total_time,
                response_length=0,
            )
            # print("report subscribe success - response_time:["+str(total_time)+"]")
        

    def locust_on_disconnect(self, client, userdata, rc):
        print("on Disconnect")
        total_time = time_delta(userdata, time.time())

        if rc == 0:
            events.request_success.fire(request_type=REQUEST_TYPE, name='disconnect', response_time=total_time, response_length=0)
        else :
            fire_locust_failure(
                request_type=REQUEST_TYPE,
                name='disconnect_fail',
                response_time=total_time,
                exception=DisconnectError("disconnected"),response_length=0
            )
            self.client.reconnect()


class MyThing(HttpUser):
    wait_time = constant(3)
    def __init__(self, *args, **kwargs):
        self.client = MQTTClient()

    @task
    class task_set(TaskSet):
        @task
        def mqtt_comm(self):
            self.client.connect_to_server()
            time.sleep(2)

#             self.client.subscribe(topic, qos=0, retry=5, name='subscribe:'+topic, timeout=SUBSCRIBE_TIMEOUT)
#             time.sleep(2)

            self.client.publish(topic, payload=payload, qos=0, name='publish:'+topic, timeout=PUBLISH_TIMEOUT)
            time.sleep(2)

            self.client.disconnect()
            time.sleep(2)


