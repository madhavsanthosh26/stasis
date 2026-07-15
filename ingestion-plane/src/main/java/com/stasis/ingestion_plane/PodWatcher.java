//our goal here is
//1. call the kubernetes api to get the list of pods 
//2. check if they are ready or not
//3. if they are not ready we will  signalevent

package com.stasis.ingestion_plane;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class PodWatcher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KubernetesClient kubernetesclient;

    public PodWatcher(KafkaTemplate<String, String> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
        this.kubernetesclient = new KubernetesClientBuilder().build();
    }
    
    @Scheduled(fixedRate = 10000)
    public void watchPods(){
        System.out.println("🔍 Watching pods...");
        try {
            kubernetesclient.pods().inNamespace("default").list().getItems().forEach(pod->{
                String podName = pod.getMetadata().getName();
                pod.getStatus().getConditions().forEach(condition -> {
                    if("Ready".equals(condition.getType()) && "False".equals(condition.getStatus())){
                        String signal = String.format("{\"service\":\"%s\",\"errorRate\":1.0}", podName);
                        kafkaTemplate.send("raw-signals", signal);
                        System.out.println("🚨 Pod not ready, signal sent: "+podName);
                    }
                });
            });
        } catch (Exception e) {
            System.out.println("❌ Error watching pods: " + e.getMessage());
        }
    }
}
