package com.example.demo.config;

import com.example.demo.po.DomainRoute;
import lombok.Data;
import lombok.ToString;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Data
@ToString
public class DomainDefineBean {
    public void setRouteMap(Map<String, String> routeMap) {
        this.routeMap = routeMap;
    }

    private Map<String, String> routeMap;

    public String getSrcDomain() {
        return srcDomain;
    }

    public void setSrcDomain(String srcDomain) {
        this.srcDomain = srcDomain;
    }

    private String srcDomain;

    public DomainRoute getDstDomainRoute(String dstDomainID){
        DomainRoute domainRoute = new DomainRoute();
        domainRoute.setBroadcast(false);
        if(routeMap.containsKey(dstDomainID)==false)
            return null;
        String domainRoute_str = routeMap.get(dstDomainID);
        List<String> domainRoute_list = Arrays.asList(domainRoute_str.split(","));
        int routeSize = domainRoute_list.size();
        for(int route_index=0; route_index<routeSize; route_index++){
            domainRoute.appendDomainIDToRoute(domainRoute_list.get(route_index));
        }
        return domainRoute;
    }

    public DomainRoute getBroadcastDomainRoute(){
        DomainRoute domainRoute = new DomainRoute();
        domainRoute.setBroadcast(true);
        return domainRoute;
    }
}
