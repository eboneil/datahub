import React, { useState } from 'react';
import styled from 'styled-components';
import { Modal, Tabs } from 'antd';
import { EmbeddedListSearchSection } from '../entity/shared/components/styled/search/EmbeddedListSearchSection';
import { UnionType } from '../search/utils/constants';
import { TestResultType } from '../../types.generated';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
`;

const StyledModal = styled(Modal)`
    top: 4vh;
    max-width: 1200px;
`;

const MODAL_WIDTH = '80vw';

const MODAL_BODY_STYLE = {
    padding: 0,
    height: '76vh',
};

const tabBarStyle = { paddingLeft: 28, paddingBottom: 0, marginBottom: 0 };

type Props = {
    urn: string;
    name: string;
    defaultActive?: TestResultType;
    passingCount: number;
    failingCount: number;
    onClose?: () => void;
};

export default function TestResultsModal({
    urn,
    name,
    defaultActive = TestResultType.Success,
    passingCount,
    failingCount,
    onClose,
}: Props) {
    const [resultType, setResultType] = useState(defaultActive);
    return (
        <StyledModal
            visible
            width={MODAL_WIDTH}
            title={<>Results - {name}</>}
            closable
            onCancel={onClose}
            bodyStyle={MODAL_BODY_STYLE}
            data-testid="test-results-modal"
            footer={null}
        >
            <Container>
                <Tabs
                    tabBarStyle={tabBarStyle}
                    defaultActiveKey={resultType}
                    activeKey={resultType}
                    size="large"
                    onTabClick={(newType) => setResultType(newType as TestResultType)}
                    onChange={(newType) => setResultType(newType as TestResultType)}
                >
                    <Tabs.TabPane tab={`Passing (${passingCount})`} key={TestResultType.Success} />
                    <Tabs.TabPane tab={`Failing (${failingCount})`} key={TestResultType.Failure} />
                </Tabs>
                <EmbeddedListSearchSection
                    defaultShowFilters
                    fixedFilters={{
                        unionType: UnionType.AND,
                        filters: [
                            {
                                field: resultType === TestResultType.Success ? 'passingTests' : 'failingTests',
                                values: [urn],
                            },
                        ],
                    }}
                />
            </Container>
        </StyledModal>
    );
}
