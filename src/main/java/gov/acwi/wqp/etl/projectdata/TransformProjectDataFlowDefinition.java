package gov.acwi.wqp.etl.projectdata;

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
public class TransformProjectDataFlowDefinition {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("truncateProjectData")
	private Tasklet truncateProjectData;

	@Autowired
	@Qualifier("transformProjectData")
	private Tasklet transformProjectData;

	@Bean
	public Step truncateProjectDataStep() {
		return stepBuilderFactory.get("truncateProjectDataStep")
				.tasklet(truncateProjectData)
				.build();
	}

	@Bean
	public Step transformProjectDataStep() {
		return stepBuilderFactory.get("transformProjectDataStep")
				.tasklet(transformProjectData)
				.build();
	}

	@Bean
	public Flow projectDataFlow() {
		return new FlowBuilder<SimpleFlow>("projectDataFlow")
				.start(truncateProjectDataStep())
				.next(transformProjectDataStep())
				.build();
	}
}
