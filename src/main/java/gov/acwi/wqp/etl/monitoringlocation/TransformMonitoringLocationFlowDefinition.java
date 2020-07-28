package gov.acwi.wqp.etl.monitoringlocation;

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
public class TransformMonitoringLocationFlowDefinition {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("truncateMonitoringLocation")
	private Tasklet truncateMonitoringLocation;

	@Autowired
	@Qualifier("transformMonitoringLocation")
	private Tasklet transformMonitoringLocation;

	@Bean
	public Step truncateMonitoringLocationStep() {
		return stepBuilderFactory.get("truncateMonitoringLocationStep")
				.tasklet(truncateMonitoringLocation)
				.build();
	}

	@Bean
	public Step transformMonitoringLocationStep() {
		return stepBuilderFactory.get("transformMonitoringLocationStep")
				.tasklet(transformMonitoringLocation)
				.build();
	}

	@Bean
	public Flow monitoringLocationFlow() {
		return new FlowBuilder<SimpleFlow>("monitoringLocationFlow")
				.start(truncateMonitoringLocationStep())
				.next(transformMonitoringLocationStep())
				.build();
	}
}
