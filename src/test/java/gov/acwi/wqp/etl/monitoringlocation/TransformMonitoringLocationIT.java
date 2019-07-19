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
			value="classpath:/testResult/stationNoSource/empty.xml"
			)
	@DatabaseSetup(
			value="classpath:/testData/monitoringLocation/"
			)
	@ExpectedDatabase(
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
