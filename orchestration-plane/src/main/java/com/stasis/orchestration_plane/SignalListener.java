package com.stasis.orchestration_plane;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;    
import org.springframework.stereotype.Component;

@Component
public class SignalListener {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final KubernetesClient kubernetesClient = new KubernetesClientBuilder().build();

    @KafkaListener(topics = "raw-signals", groupId = "stasis-orchestration")
    public void onSignal(String message){
        try{
            SignalEvent signal = objectMapper.readValue(message, SignalEvent.class);
            System.out.println("Service: "+ signal.getService());
            System.out.println("Error Rate: " + signal.getErrorRate());

            if(signal.getErrorRate() > 0.7){
                System.out.println("Anomaly detected in: " + signal.getService());
                restartPod(signal.getService());
            }
        }catch(Exception e){
            System.out.println("Failed to parse signal: " + e.getMessage());
        }
    }

    private void restartPod(String podName){
        try{
            kubernetesClient.pods().inNamespace("default").withName(podName).delete();
            System.out.println("Restarted Pod: "+podName);
            writeAuditLog(podName);
        }catch(Exception e){
            System.out.println("Failed to restart pod: "+ e.getMessage());
        }
        
    }

    private void writeAuditLog(String podName){
        try{
            String entry = String.format("[%s] AUTO-REMEDIATED %s %n",  java.time.LocalDateTime.now(), podName);
            java.nio.file.Files.writeString(java.nio.file.Paths.get("audit.log"),entry, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
            System.out.println("Audit log updated!");
        }catch(Exception e){
            System.out.println("Failed to write the audit log: "+e.getMessage());
        }
    }
}