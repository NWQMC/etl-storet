package gov.acwi.wqp.etl.activity;

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
public class TransformActivityFlowDefinition {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("truncateActivity")
	private Tasklet truncateActivity;

	@Autowired
	@Qualifier("transformActivity")
	private Tasklet transformActivity;

	@Bean
	public Step truncateActivityStep() {
		return stepBuilderFactory.get("truncateActivityStep")
				.tasklet(truncateActivity)
				.build();
	}

	@Bean
	public Step transformActivityStep() {
		return stepBuilderFactory.get("transformActivityStep")
				.tasklet(transformActivity)
				.build();
	}

	@Bean
	public Flow activityFlow() {
		return new FlowBuilder<SimpleFlow>("activityFlow")
				.start(truncateActivityStep())
				.next(transformActivityStep())
				.build();
	}
}
