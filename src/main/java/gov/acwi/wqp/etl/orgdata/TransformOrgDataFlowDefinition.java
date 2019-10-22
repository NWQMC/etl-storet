package gov.acwi.wqp.etl.orgdata;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TransformOrgDataFlowDefinition {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("truncateOrgData")
	private Tasklet truncateOrgData;

	@Autowired
	@Qualifier("transformOrgData")
	private Tasklet transformOrgData;

	@Bean
	public Step truncateOrgDataStep() {
		return stepBuilderFactory.get("truncateOrgDataStep")
				.tasklet(truncateOrgData)
				.build();
	}

	@Bean
	public Step transformOrgDataStep() {
		return stepBuilderFactory.get("transformOrgDataStep")
				.tasklet(transformOrgData)
				.build();
	}

	@Bean
	public Flow orgDataFlow() {
		return new FlowBuilder<SimpleFlow>("orgDataFlow")
				.start(truncateOrgDataStep())
				.next(transformOrgDataStep())
				.build();
	}
}
