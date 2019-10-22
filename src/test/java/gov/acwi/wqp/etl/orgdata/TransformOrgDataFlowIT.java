package gov.acwi.wqp.etl.orgdata;

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

public class TransformOrgDataFlowIT extends StoretBaseFlowIT {

	@Autowired
	@Qualifier("orgDataFlow")
	private Flow orgDataFlow;

	@Before
	public void setUp() {
		testJob = jobBuilderFactory.get("orgDataFlowTest")
				.start(orgDataFlow)
				.build()
				.build();
		jobLauncherTestUtils.setJob(testJob);
	}

	@Test
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testData/orgDataNoSource.xml"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW_DUMP,
			value="classpath:/testData/orgData/"
			)
	@ExpectedDatabase(
			connection=CONNECTION_STORETW,
			value="classpath:/testResult/orgDataNoSource/csv/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	public void flowTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}
