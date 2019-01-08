package com.example.demo.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({
        DomainCollectionConfig.class
})
public class DomainDefineBeanConfiguration {
    @Autowired
    private DomainCollectionConfig domainCollectionConfig;

    @Bean(name="domainBean")
    @ConditionalOnMissingBean
    public DomainDefineBean domainBean(){
        DomainDefineBean domainDefineBean = new DomainDefineBean();
        domainDefineBean.setRouteMap(domainCollectionConfig.getMap());
        domainDefineBean.setSrcDomain(domainCollectionConfig.getSrcDomain());
        return domainDefineBean;
    }
}
