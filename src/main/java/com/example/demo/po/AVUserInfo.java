package com.example.demo.po;

import java.io.Serializable;

public class AVUserInfo implements Serializable {

    public String getClient_id() {
        return client_id;
    }

    public void setClient_id(String client_id) {
        this.client_id = client_id;
    }

    public String getClient_name() {
        return client_name;
    }

    public void setClient_name(String client_name) {
        this.client_name = client_name;
    }

    public String getBinding_key() {
        return binding_key;
    }

    public void setBinding_key(String binding_key) {
        this.binding_key = binding_key;
    }

    private String client_id = "";
    private String client_name = "";
    private String binding_key = "";

    public String getSrc_domain() {
        return src_domain;
    }

    public void setSrc_domain(String src_domain) {
        this.src_domain = src_domain;
    }

    //用户当前登录的域ID
    private String src_domain = "";
}
