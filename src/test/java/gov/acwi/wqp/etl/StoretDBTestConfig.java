package gov.acwi.wqp.etl;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import com.github.springtestdbunit.bean.DatabaseConfigBean;
import com.github.springtestdbunit.bean.DatabaseDataSourceConnectionFactoryBean;

@TestConfiguration
public class StoretDBTestConfig {

	@Autowired
	private DataSource dataSource;

	@Bean
	public DatabaseConfigBean dbUnitDatabaseConfig() {
		DatabaseConfigBean dbUnitDbConfig = new DatabaseConfigBean();
		dbUnitDbConfig.setDatatypeFactory(new PostgresqlDataTypeFactory());
		return dbUnitDbConfig;
	}

	@Bean
	public DatabaseDataSourceConnectionFactoryBean storetw() throws SQLException {
		DatabaseDataSourceConnectionFactoryBean dbUnitDatabaseConnection = new DatabaseDataSourceConnectionFactoryBean();
		dbUnitDatabaseConnection.setDatabaseConfig(dbUnitDatabaseConfig());
		dbUnitDatabaseConnection.setDataSource(dataSource);
		dbUnitDatabaseConnection.setSchema("storetw");
		return dbUnitDatabaseConnection;
	}

	@Bean
	public DatabaseDataSourceConnectionFactoryBean storetwDump(DatabaseConfigBean dbUnitDatabaseConfig) throws SQLException {
		//Currently the STORETW tables from EPA are in upper case.
		dbUnitDatabaseConfig.setCaseSensitiveTableNames(true);
		//And need to be enclosed in quotes for the delete portion of the database setup.
		dbUnitDatabaseConfig.setEscapePattern("\"?\"");
		DatabaseDataSourceConnectionFactoryBean dbUnitDatabaseConnection = new DatabaseDataSourceConnectionFactoryBean();
		dbUnitDatabaseConnection.setDatabaseConfig(dbUnitDatabaseConfig);
		dbUnitDatabaseConnection.setDataSource(dataSource);
		dbUnitDatabaseConnection.setSchema("storetw_dump");
		return dbUnitDatabaseConnection;
	}

	@Bean
	public DatabaseDataSourceConnectionFactoryBean wqx() throws SQLException {
		DatabaseDataSourceConnectionFactoryBean dbUnitDatabaseConnection = new DatabaseDataSourceConnectionFactoryBean();
		dbUnitDatabaseConnection.setDatabaseConfig(dbUnitDatabaseConfig());
		dbUnitDatabaseConnection.setDataSource(dataSource);
		dbUnitDatabaseConnection.setSchema("wqx");
		return dbUnitDatabaseConnection;
	}
}
