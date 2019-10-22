package gov.acwi.wqp.etl.monitoringlocation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.StoretBaseFlowIT;

public class TransformMonitoringLocationIT extends StoretBaseFlowIT {

	@Test
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testResult/stationNoSource/empty.xml"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testData/storetwTransition/"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW_DUMP,
			value="classpath:/testData/orgData/"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW_DUMP,
			value="classpath:/testData/monitoringLocation/"
			)
	@ExpectedDatabase(
			connection=CONNECTION_STORETW,
			value="classpath:/testResult/stationNoSource/csv/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	public void transformTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformMonitoringLocationStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

}
