package com.azure.cosmos.examples.common;

public class ItemWithMetaData {

    public ItemWithMetaData() {
    }

    public String getId() {
        return id;
    }
    public String getMyPk() {
        return myPk;
    }
    public Metadata getMetadata() {
        return _metadata;
    }        

    public String id;
    public String myPk;
    public Metadata _metadata;

}
