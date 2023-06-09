import { SelectTypeStep } from './steps/SelectTypeStep';
import { ConfigureDatasetSlaAssertionStep } from './steps/ConfigureDatasetSlaAssertionStep';
import { ConfigureEvaluationScheduleStep } from './steps/ConfigureEvaluationScheduleStep';

/**
 * Mapping from the step type to the component implementing that step.
 */
export const AssertionsBuilderStepComponent = {
    SELECT_TYPE: SelectTypeStep,
    CONFIGURE_DATASET_SLA_ASSERTION: ConfigureDatasetSlaAssertionStep,
    CONFIGURE_SCHEDULE: ConfigureEvaluationScheduleStep,
};

/**
 * Mapping from the step type to the title for the step
 */
export enum AssertionBuilderStepTitles {
    SELECT_TYPE = 'Select Assertion Type',
    CONFIGURE_DATASET_SLA_ASSERTION = 'Configure Assertion',
    CONFIGURE_SCHEDULE = 'Finish up',
}
