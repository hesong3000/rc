package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class DemoApplication {

	public static void main(String[] args) throws InterruptedException {
		SpringApplication app = new SpringApplication(DemoApplication.class);
		ConfigurableApplicationContext context = app.run(args);
		RoomControllerRecvThread roomControllerRecvThread = (RoomControllerRecvThread)context.getBean("roomControllerRecvThread");
		roomControllerRecvThread.start();
		roomControllerRecvThread.join();
		context.close();
	}
}
