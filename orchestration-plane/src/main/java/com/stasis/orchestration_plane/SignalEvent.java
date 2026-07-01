package com.stasis.orchestration_plane;


public class SignalEvent {
    private String service;
    private Double errorRate;

    public String getService(){
        return service;
    }
    public Double getErrorRate(){
        return errorRate;
    }
}
