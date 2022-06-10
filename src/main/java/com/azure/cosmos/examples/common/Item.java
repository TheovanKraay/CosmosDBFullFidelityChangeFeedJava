package com.azure.cosmos.examples.common;

public class Item {

    public Item() {
    }

    public String getId() {
        return id;
    }
    public String getMypk() {
        return mypk;
    }
    public String getProp() {
        return prop;
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
}
