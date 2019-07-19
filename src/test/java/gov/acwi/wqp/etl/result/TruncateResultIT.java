package gov.acwi.wqp.etl.result;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.StoretBaseFlowIT;

public class TruncateResultIT extends StoretBaseFlowIT {

	@Test
	@DatabaseSetup(
			value="classpath:/testData/resultNoSource.xml"
			)
	@ExpectedDatabase(
			value="classpath:/testResult/resultNoSource/empty.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	public void truncateTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("truncateResultStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}
