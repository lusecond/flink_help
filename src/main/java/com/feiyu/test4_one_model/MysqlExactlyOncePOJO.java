package com.feiyu.test4_one_model;

//@Data
//@NoArgsConstructor
//@AllArgsConstructor
public class MysqlExactlyOncePOJO {
    public MysqlExactlyOncePOJO(String value) {
        this.value = value;
    }

    public MysqlExactlyOncePOJO() {
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    private String value;
}