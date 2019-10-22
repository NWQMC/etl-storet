package gov.acwi.wqp.etl.activity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.StoretBaseFlowIT;

public class TransformActivityIT extends StoretBaseFlowIT {

	@Test
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testResult/activityNoSource/empty.xml"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testResult/resultNoSource/csv/"
			)
	@ExpectedDatabase(
			connection=CONNECTION_STORETW,
			value="classpath:/testResult/activityNoSource/csv/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	public void transformTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformActivityStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

}
