package com.feiyu.gflow.test2.test;

import java.io.Serializable;

public class StringSerial implements Serializable {
    public StringSerial(String string) {
        this.string = string;
    }

    public StringSerial() {
    }

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    private String string;
//    private transient String string;
}
