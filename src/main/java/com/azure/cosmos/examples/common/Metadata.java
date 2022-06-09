package com.azure.cosmos.examples.common;

public class Metadata {

    public String getOperationType() {
        return operationType;
    }
    public Boolean getTimeToLiveExpired() {
        return timeToLiveExpired;
    }
    public Item getPreviousImage() {
        return previousImage;
    }
    public String operationType;
    public Boolean timeToLiveExpired;
    public Item previousImage;

}
