# mqtt-locust
Load Testing using Locust and Paho MQTT Client:

Dependencies : 
Python 3.6+,
Locust == 1.2.3,
PAHO MQTT Client (pip),
InfluxDB Client (pip),
Grafana (For Live Monitoring)

Set the mqtt broker name and port in the MQTTClient Class and the script is all set up and ready for testing.

locust --host=<broker> -f mqtt_locust_test.py
