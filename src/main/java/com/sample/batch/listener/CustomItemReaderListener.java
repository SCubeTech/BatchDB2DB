package com.sample.batch.listener;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.sample.batch.model.Person;

public class CustomItemReaderListener implements ItemReadListener<Person> {
	
	
	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public CustomItemReaderListener(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}


	@Override
	public void beforeRead() {
		System.out.println("ItemReadListener - beforeRead");
	}


	@Override
	public void onReadError(Exception ex) {
		System.out.println("ItemReadListener - onReadError");
	}

	@Override
	public void afterRead(Person item) {
		jdbcTemplate.update("update ord set isUpdated='Y' where id = "+item.getId());
		System.out.println(item.getId()+"-------"+item.getOrderId());
		
	}

}