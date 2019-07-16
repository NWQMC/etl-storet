package gov.acwi.wqp.etl.result;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.StoretBaseFlowIT;

public class TransformResultFlowIT extends StoretBaseFlowIT {

	@Autowired
	@Qualifier("resultFlow")
	private Flow resultFlow;

	@Before
	public void setUp() {
		testJob = jobBuilderFactory.get("resultFlowTest")
				.start(resultFlow)
				.build()
				.build();
		jobLauncherTestUtils.setJob(testJob);
	}

	@Test
	@DatabaseSetup(
			value="classpath:/testData/resultNoSource.xml"
			)
	@DatabaseSetup(
			value="classpath:/testData/result/"
			)
	@DatabaseSetup(
			value="classpath:/testResult/stationNoSource/csv/"
			)
	@DatabaseSetup(
			value="classpath:/testData/result/"
			)
	@ExpectedDatabase(
			value="classpath:/testResult/resultNoSource/csv/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	public void transformResultFlowTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}