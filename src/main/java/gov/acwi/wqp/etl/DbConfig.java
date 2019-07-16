package gov.acwi.wqp.etl;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

//@Configuration
public class DbConfig {

	@Bean
	@Primary
	public DataSourceProperties dataSourceProperties() {
		return new DataSourceProperties();
	}

	@Bean
	@Primary
	public DataSource dataSource() {
		return dataSourceProperties().initializeDataSourceBuilder().build();
	}

	@Bean
	@Primary
	public JdbcTemplate jdbcTemplate() {
		return new JdbcTemplate(dataSource());
	}

	@Bean
	@ConfigurationProperties(prefix="spring.datasource-wqp")
	public DataSourceProperties dataSourcePropertiesWqp() {
		return new DataSourceProperties();
	}

	@Bean
	public DataSource dataSourceWqp() {
		return dataSourcePropertiesWqp().initializeDataSourceBuilder().build();
	}
}
