package applications;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import zipkin.server.EnableZipkinServer;

@SpringBootApplication
@EnableZipkinServer
public class ApplicationContextSpring {

    public static void main(String[] args){
        SpringApplication.run(ApplicationContextSpring.class, args);
    }

}