package com.mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class ClientMQTT {
	
	public static void main(String[] args) {
		
		
		
		// Creating a MQTT Client using Eclipse Paho
		String topic = "news";
		String content = "Visit www.hascode.com! :D";
		int qos = 2;
		String broker = "tcp://0.0.0.0:1884";
		String clientId = "paho-java-client";
		try {
			MqttClient sampleClient = new MqttClient(broker, clientId, new MemoryPersistence());
			MqttConnectOptions connOpts = new MqttConnectOptions();
			connOpts.setCleanSession(true);
			System.out.println("paho-client connecting to broker: " + broker);
			sampleClient.connect(connOpts);
			System.out.println("paho-client connected to broker");
			System.out.println("paho-client publishing message: " + content);
			MqttMessage message = new MqttMessage(content.getBytes());
			message.setQos(qos);
			for(int i=0;i<100;i++)
			sampleClient.publish(topic, message);
			System.out.println("paho-client message published");
			sampleClient.disconnect();
			System.out.println("paho-client disconnected");
		} catch (MqttException me) {
			me.printStackTrace();
		}

		DataStore ds=new DataStore();
		ds.selectTable();
	}

}
