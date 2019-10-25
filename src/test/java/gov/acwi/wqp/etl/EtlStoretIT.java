package gov.acwi.wqp.etl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

public class EtlStoretIT extends StoretBaseFlowIT {

	@Test
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testData/storetwTransition/"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testData/orgDataNoSource.xml"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testData/projectDataNoSource.xml"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testData/stationNoSource.xml"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testData/activityNoSource.xml"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testData/resultNoSource.xml"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW_DUMP,
			value="classpath:/testData/orgData/"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW_DUMP,
			value="classpath:/testData/projectData/"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW_DUMP,
			value="classpath:/testData/monitoringLocation/"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW_DUMP,
			value="classpath:/testData/result/"
			)
	@DatabaseSetup(
			connection=CONNECTION_WQX,
			value="classpath:/testData/nemi/"
			)

	@ExpectedDatabase(
			connection=CONNECTION_STORETW,
			value="classpath:/testResult/orgDataNoSource/csv/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	@ExpectedDatabase(
			connection=CONNECTION_STORETW,
			value="classpath:/testResult/projectDataNoSource/csv/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	@ExpectedDatabase(
			connection=CONNECTION_STORETW,
			value="classpath:/testResult/stationNoSource/csv/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	@ExpectedDatabase(
			connection=CONNECTION_STORETW,
			value="classpath:/testResult/resultNoSource/csv/",
			table="result_no_source",
			query="select * from result_no_source order by result_id"
			)
	@ExpectedDatabase(
			connection=CONNECTION_STORETW,
			value="classpath:/testResult/activityNoSource/csv/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	public void endToEndTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
			Thread.sleep(1000);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

}
