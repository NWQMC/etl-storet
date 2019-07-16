package gov.acwi.wqp.etl.result;

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
public class TransformResultFlowDefinition {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("truncateResult")
	private Tasklet truncateResult;

	@Autowired
	@Qualifier("transformResult")
	private Tasklet transformResult;

	@Bean
	public Step truncateResultStep() {
		return stepBuilderFactory.get("truncateResultStep")
				.tasklet(truncateResult)
				.build();
	}

	@Bean
	public Step transformResultStep() {
		return stepBuilderFactory.get("transformResultStep")
				.tasklet(transformResult)
				.build();
	}

	@Bean
	public Flow resultFlow() {
		return new FlowBuilder<SimpleFlow>("resultFlow")
				.start(truncateResultStep())
				.next(transformResultStep())
				.build();
	}
}
