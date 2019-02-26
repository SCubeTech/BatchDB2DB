package com.sample.batch.processor;

import org.springframework.batch.item.ItemProcessor;

import com.sample.batch.model.Person;

public class PersonItenProcessor implements ItemProcessor<Person, Person>{

	@Override
	public Person process(Person person) throws Exception {
		String orderId = person.getOrderId();
		orderId = orderId.replace("PO", "SO");
		person.setOrderId(orderId);
		return person;
	}
}
