package com.mqtt;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.binary.StringUtils;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.InterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.server.Server;
import io.moquette.server.config.ClasspathConfig;
import io.moquette.server.config.IConfig;

public class Main {

	static class PublisherListener extends AbstractInterceptHandler {
		@Override
		public void onPublish(InterceptPublishMessage message) {
			
			String topic=message.getTopicName();
			String payload=new String(message.getPayload().array());
				
			System.out.println("moquette mqtt broker message intercepted, topic: " +topic
					+ ", content: " +payload);
			
			
			DataStore ds=new DataStore();
			String sql = "INSERT INTO HEARTBEAT(topic,message) "
					+ "VALUES ('"+topic+"','"+payload+"')";			
			ds.storeData(sql);
			
			
			if (DataPattern.series != null) {
				
				DataPattern.series = DataPattern.series + payload;
				if(DataPattern.count>100)DataPattern.series=null;
				if(DataPattern.series.contains("fail")){
					DataPattern.alert=true;
					System.out.println(DataPattern.alert);
				}

			}
			
			
		}
	}

	public static void main(String[] args) throws InterruptedException, IOException {
		// Creating a MQTT Broker using Moquette
		final IConfig classPathConfig = new ClasspathConfig();

		final Server mqttBroker = new Server();
		final List<? extends InterceptHandler> userHandlers = Arrays.asList(new PublisherListener());
		mqttBroker.startServer(classPathConfig, userHandlers);
		
		DataStore ds=new DataStore();
		ds.deleteTable();
		ds.createTable();

		System.out.println("moquette mqtt broker started, press ctrl-c to shutdown..");
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("stopping moquette mqtt broker..");
				mqttBroker.stopServer();
				System.out.println("moquette mqtt broker stopped");
			}
		});

		Thread.sleep(4000);

	}
}