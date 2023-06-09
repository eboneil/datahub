import React, { useState } from 'react';
import styled from 'styled-components';
import { Steps } from 'antd';
import { AssertionMonitorBuilderState, StepProps, AssertionBuilderStep } from './types';
import { AssertionBuilderStepTitles, AssertionsBuilderStepComponent } from './conf';
import { DEFAULT_BUILDER_STATE } from './constants';
import { EntityType, Monitor } from '../../../../../../../../types.generated';
import { useCreateAssertionMonitor } from './useCreateAssertionMonitor';

const Container = styled.div`
    display: flex;
    align-items: top;
    justify-content: space-between;
    width: 100%;
    min-height: 50vh;
`;

const MainContent = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
`;

const StepsContainer = styled.div`
    margin-right: 20px;
    margin-left: 20px;
    margin-bottom: 40px;
`;

const stepIds = Object.values(AssertionBuilderStep);
const stepIndexToId = new Map();
stepIds.forEach((stepId, index) => stepIndexToId.set(stepId, index));

type Props = {
    entityUrn: string;
    entityType: EntityType;
    platformUrn: string;
    initialState?: AssertionMonitorBuilderState;
    onSubmit?: (monitor: Monitor) => void;
    onCancel?: () => void;
};

export const AssertionMonitorBuilder = ({
    entityUrn,
    entityType,
    platformUrn,
    initialState,
    onSubmit,
    onCancel,
}: Props) => {
    const [stepStack, setStepStack] = useState<AssertionBuilderStep[]>([AssertionBuilderStep.SELECT_TYPE]);
    const [builderState, setBuilderState] = useState<AssertionMonitorBuilderState>(
        initialState || { ...DEFAULT_BUILDER_STATE, entityUrn, entityType, platformUrn },
    );

    const onCreateAssertionMonitor = (newMonitor: Monitor) => {
        onSubmit?.(newMonitor);
        setBuilderState(DEFAULT_BUILDER_STATE);
    };

    const createAssertionMonitor = useCreateAssertionMonitor(builderState, onCreateAssertionMonitor);

    /**
     * The current step id, e.g. SELECT_TYPE
     */
    const currentStep = stepStack[stepStack.length - 1];
    /**
     * The current step index, e.g. 0
     */
    const currentStepIndex = stepIndexToId.get(currentStep);
    /**
     * The current step component
     */
    const StepComponent: React.FC<StepProps> = AssertionsBuilderStepComponent[currentStep];

    const goTo = (step: AssertionBuilderStep) => {
        setStepStack([...stepStack, step]);
    };

    const prev = () => {
        setStepStack(stepStack.slice(0, -1));
    };

    const cancel = () => {
        onCancel?.();
    };

    return (
        <Container>
            <MainContent>
                <StepsContainer>
                    <Steps current={currentStepIndex}>
                        {stepIds.map((id) => (
                            <Steps.Step key={id} title={AssertionBuilderStepTitles[id]} />
                        ))}
                    </Steps>
                </StepsContainer>
                <StepComponent
                    state={builderState}
                    updateState={setBuilderState}
                    goTo={goTo}
                    prev={stepStack.length > 1 ? prev : undefined}
                    submit={createAssertionMonitor}
                    cancel={cancel}
                />
            </MainContent>
        </Container>
    );
};
