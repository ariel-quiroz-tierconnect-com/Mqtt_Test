/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ariel.mosquittotest;

import java.util.Random;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 *
 * @author aquiroz
 */
public class MqttMain {

    static final String TOPIC = "/v1/data/ALEB/default_rfid_thingtype";
    static final int QOS = 2;
    static final String BROKER = "tcp://localhost:1883";
    static final String CLIENT_ID = "HELPER_PAHO";
    static final int SINCE = 0;
    static final int UNTIL = 49;

    public static void main(String args[]) {

        new MqttMain().execute();

    }

    public void execute(){
        MemoryPersistence persistence = new MemoryPersistence();

        String content = buildMessage();

        MqttClient sampleClient;
        try {
            sampleClient = new MqttClient(BROKER, CLIENT_ID, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            System.out.println("Connecting to broker: " + BROKER);
            sampleClient.connect(connOpts);
            System.out.println("Connected");
            System.out.println("Publishing message: " + content);
            MqttMessage message = new MqttMessage(content.getBytes());
            message.setQos(QOS);
            sampleClient.publish(TOPIC, message);
            System.out.println("Message published!!!");
            sampleClient.disconnect();
            System.out.println("Disconnected");
            System.exit(0);
        } catch (MqttException ex) {
            System.out.println("Error" + ex);
        }
    }
    
    private int generateNumberRandom() {
        Random rnd = new Random();
        int number = rnd.nextInt(UNTIL - SINCE + 1) + SINCE;
        return number;
    }
    
    private String buildMessage(){
        StringBuilder message=new StringBuilder();
        message .append("sn,22836").append("\n")
                .append(",0,___CS___,-118.443969;34.048092;0.0;20.0;ft").append("\n");
        
        int posBad=generateNumberRandom();
        long time=System.currentTimeMillis();
        for(int i=0;i<50;i++){
            if (i==posBad) {                
                message.append("MNS00090,1470167400939,lastDetectTime,").append(time).append("\n");
            }else{
                message.append("MNS00091,1470167400939,lastDetectTime,").append(time).append("\n");
            }
        }                       
        return message.toString();
    }

}
