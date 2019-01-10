package com.example.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class DemoApplication {

	public static void main(String[] args) throws InterruptedException {
//		String ss = "{\"candidate\":\"{\\\"candidate\\\":\\\"a=candidate:3 1 udp 2013266429 10.25.8.115 47734 typ host generation 0\\\",\\\"sdpMLineIndex\\\":0,\\\"sdpMid\\\":\\\"0\\\"}}";
//		ss = ss.replaceAll("\\\\", "");
//		ss = ss.replaceAll("\"\\{","{");
//		ss = ss.replaceAll("\\}\"","}");
//		System.out.println(ss);
//		JSONObject aa = JSON.parseObject(ss);
		SpringApplication app = new SpringApplication(DemoApplication.class);
		ConfigurableApplicationContext context = app.run(args);
		RoomControllerRecvThread roomControllerRecvThread = (RoomControllerRecvThread)context.getBean("roomControllerRecvThread");
		roomControllerRecvThread.start();
		roomControllerRecvThread.join();
		context.close();
	}
}
