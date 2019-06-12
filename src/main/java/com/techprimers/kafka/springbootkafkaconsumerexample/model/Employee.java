package com.techprimers.kafka.springbootkafkaconsumerexample.model;


import org.springframework.data.annotation.Id;
import javax.persistence.Entity;

@Entity
public class Employee {
	
	@Id
	private long empId;
	private String name;
	private String position;
	private String dept;
	private long salary;
	private String company;
	
	public Employee() {
		super();
	}
	public Employee(long empId, String name, String position, String dept,
			long salary, String company) {
		super();
		this.empId = empId;
		this.name = name;
		this.position = position;
		this.dept = dept;
		this.salary = salary;
		this.company = company;
	}
	public long getEmpId() {
		return empId;
	}
	public void setEmpId(long empId) {
		this.empId = empId;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getPosition() {
		return position;
	}
	public void setPosition(String position) {
		this.position = position;
	}
	public String getDept() {
		return dept;
	}
	public void setDept(String dept) {
		this.dept = dept;
	}
	public long getSalary() {
		return salary;
	}
	public void setSalary(long salary) {
		this.salary = salary;
	}
	public String getCompany() {
		return company;
	}
	public void setCompany(String company) {
		this.company = company;
	}
	@Override
	public String toString() {
		return "Employee [empId=" + empId + ", name=" + name + ", position="
				+ position + ", dept=" + dept + ", salary=" + salary
				+ ", company=" + company + "]";
	}
	

}
