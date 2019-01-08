package com.example.demo.po;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class DomainRoute implements Serializable {
    public DomainRoute(){}
    public DomainRoute(DomainRoute domainRoute){
        this.isBroadcast = domainRoute.isBroadcast();
        int size = domainRoute.getDomainRoute().size();
        for(int index=0; index<size; index++){
            this.domainRoute.add(domainRoute.getDomainRoute().get(index));
        }
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    public void setBroadcast(boolean broadcast) {
        isBroadcast = broadcast;
    }

    public String getHeadDomainID(){
        if(domainRoute.isEmpty())
            return "";
        return domainRoute.get(0);
    }

    public int getRouteTTL(){
        return domainRoute.size();
    }

    public void appendDomainIDToRoute(String DomainID){
        domainRoute.add(DomainID);
    }

    public void removeHeadDomianID(String DomainID){
        if(domainRoute.isEmpty())
            return;
        if(domainRoute.get(0).compareTo(DomainID)!=0)
            return;
        domainRoute.remove(0);
    }

    private boolean isBroadcast = false;

    public List<String> getDomainRoute() {
        return domainRoute;
    }

    public void setDomainRoute(List<String> domainRoute) {
        //this.domainRoute = domainRoute;
        this.domainRoute.clear();
        this.domainRoute.addAll(domainRoute);
    }

    private List<String> domainRoute = new LinkedList<>();
}

