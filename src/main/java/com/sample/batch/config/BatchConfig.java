package com.sample.batch.config;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.sample.batch.listener.CustomItemReaderListener;
import com.sample.batch.model.Person;
import com.sample.batch.processor.PersonItenProcessor;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;


	@Autowired
	private JdbcTemplate jdbcTemplate;

	/**
	 * Primary datasource set as HSQL
	 * 
	 * @return
	 */
	@Bean
	@Primary
	@ConfigurationProperties(prefix = "spring.batchdatasource")
	public DataSource h2DataSource() {
		return DataSourceBuilder.create().build();
	}

	@Bean
	BatchConfigurer configurer(DataSource dataSource) {
		return new DefaultBatchConfigurer(dataSource);
	}

	@Bean(name = "hsqldbDataSource")
	@ConfigurationProperties(prefix = "spring.hsqldbdatasource")
	public DataSource hsqldbdataSource() {
		return DataSourceBuilder.create().build();
	}
	
	@Bean(name = "mssqlDataSource")
	@ConfigurationProperties(prefix = "spring.mysqldatasource")
	public DataSource dataSource() {
		return DataSourceBuilder.create().build();
	}
	
	@Bean(name="mssqljdbcTemplate")
	public JdbcTemplate mssqljdbcTemplate(@Qualifier("mssqlDataSource") final DataSource dataSource){
		JdbcTemplate jdbcTemplate = new JdbcTemplate();
		jdbcTemplate.setDataSource(dataSource);
		return jdbcTemplate;
	}
	
	@Bean
	public JdbcCursorItemReader<Person> reader(@Qualifier("mssqlDataSource") final DataSource dataSource) {
		JdbcCursorItemReader<Person> cursorItemReader = new JdbcCursorItemReader<>();
		cursorItemReader.setDataSource(dataSource);
		cursorItemReader.setSql("SELECT id,orderId FROM ord");
		cursorItemReader.setRowMapper(new PersonRowMapper());
		return cursorItemReader;
	}

	@Bean
	public PersonItenProcessor processor() {
		return new PersonItenProcessor();
	}

	@Bean
	public CustomItemReaderListener listener() {
		return new CustomItemReaderListener(mssqljdbcTemplate(dataSource()));
	}

	/*Writing in text file.
	 * @Bean
	public FlatFileItemWriter<Person> writer() {
		FlatFileItemWriter<Person> writer = new FlatFileItemWriter<Person>();
		writer.setResource(new ClassPathResource("persons.csv"));

		DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<Person>();
		lineAggregator.setDelimiter(",");

		BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<Person>();
		fieldExtractor.setNames(new String[] { "id", "orderId" });
		lineAggregator.setFieldExtractor(fieldExtractor);

		writer.setLineAggregator(lineAggregator);
		return writer;
	}
	*/
	
	@Bean
    public JdbcBatchItemWriter<Person> writer() {
        JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<Person>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
        writer.setSql("INSERT INTO PUBLIC.PERSON (ID, ORDERID, ISUPDATED) VALUES (:id, :orderId,:isUpdated)");
        writer.setDataSource(hsqldbdataSource());
        return writer;
    }

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1").<Person, Person>chunk(1).reader(reader(dataSource())).processor(processor())
				.writer(writer()).listener(listener()).build();
	}

	@Bean
	public Job exportPerosnJob() {
		return jobBuilderFactory.get("exportPeronJob").incrementer(new RunIdIncrementer()).flow(step1()).end().build();
	}

}

class PersonRowMapper implements RowMapper<Person> {

	@Override
	public Person mapRow(ResultSet rs, int rowNum) throws SQLException {
		Person person = new Person();
		person.setId(rs.getString("id"));
		person.setOrderId(rs.getString("orderId"));
		return person;
	}

}
