package com.techprimers.kafka.springbootkafkaconsumerexample.listener;

import com.techprimers.kafka.springbootkafkaconsumerexample.model.Employee;
import com.techprimers.kafka.springbootkafkaconsumerexample.model.User;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

	@Autowired
	private MongoTemplate mongoTemplate;

	@KafkaListener(topics = "Kafka_Example", group = "group_id")
	public void consume(String message) {
		System.out.println("Consumed message: " + message);
	}


	@KafkaListener(topics = "Kafka_Example_json", group = "group_json",
			containerFactory = "userKafkaListenerFactory")
	public void consumeJson(User user) {
		System.out.println("Consumed JSON Message: " + user);
	}

	@KafkaListener(topics = "Employee", group = "group_json",
			containerFactory = "empKafkaListenerFactory")
	public void consumeJsonEmp(Employee emp) {
		System.out.println("Consumed JSON Message: " + emp);
		saveUser(emp);
	}

	public String saveUser( Employee emp) {

		Employee emp1=mongoTemplate.findById(emp.getEmpId(), Employee.class);
		
		if(emp1!=null) {
			
			return "Already Employee Exist";
		}
		
		mongoTemplate.save(emp);
			
		return "Data saved successfully";
		
	}

}
