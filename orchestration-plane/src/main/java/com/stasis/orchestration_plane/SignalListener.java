package com.stasis.orchestration_plane;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SignalListener {
    private final ObjectMapper objectMapper = new ObjectMapper();
    @KafkaListener(topics = "raw-signals", groupId = "stasis-orchestration")
    public void onSignal(String message){
        try{
            SignalEvent signal = objectMapper.readValue(message, SignalEvent.class);
            System.out.println("Service: "+ signal.getService());
            System.out.println("Error Rate: " + signal.getErrorRate());
            if(signal.getErrorRate() > 0.7){
                System.out.println("Anomaly detected in: " + signal.getService());
            }
        }catch(Exception e){
            System.out.println("Failed to parse signal: " + e.getMessage());
        }
    }
}