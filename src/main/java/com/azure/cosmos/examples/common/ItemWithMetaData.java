package com.azure.cosmos.examples.common;

public class ItemWithMetaData {

    public ItemWithMetaData() {
    }

    public String getId() {
        return id;
    }
    public String getMyPk() {
        return mypk;
    }
    public String getProp() {
        return prop;
    }    
    public Metadata getMetadata() {
        return _metadata;
    }        

    public String id;
    public String mypk;
    public String prop;
    public String _rid;
    public String _attachments;
    public String _ts;
    public String _lsn;
    public String _etag;
    public String _self;
    public String someProperty;
    public Metadata _metadata;

}
