package com.stasis.orchestration_plane;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;    
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
@Component
public class SignalListener {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final KubernetesClient kubernetesClient = new KubernetesClientBuilder().build();
    private Map<String, LocalDateTime> cooldownMap = new HashMap<>();

    @KafkaListener(topics = "raw-signals", groupId = "stasis-orchestration")
    public void onSignal(String message){
        try{
            SignalEvent signal = objectMapper.readValue(message, SignalEvent.class);
            System.out.println("Service: "+ signal.getService());
            System.out.println("Error Rate: " + signal.getErrorRate());

            if(signal.getErrorRate() > 0.7){
                System.out.println("Anomaly detected in: " + signal.getService());
                String serviceName = extractServiceName(signal.getService());

                LocalDateTime lastRestart = cooldownMap.get(serviceName);
                if(lastRestart != null && lastRestart.isAfter(LocalDateTime.now().minusSeconds(60))){
                    System.out.println("Cooldown is active, Skipping the restart of "+ serviceName);
                    return;
                }
                
                restartPod(signal.getService());
                cooldownMap.put(serviceName, LocalDateTime.now());
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
    
    private String extractServiceName(String podName){
        String[] parts = podName.split("-");
        return parts[0];
    }
}