<<<<<<< HEAD
import { FileDoneOutlined, FileProtectOutlined, PlusOutlined } from '@ant-design/icons';
=======
import React, { useEffect } from 'react';
>>>>>>> oss_master
import { Button } from 'antd';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';
import { FileDoneOutlined, FileProtectOutlined } from '@ant-design/icons';
import { useEntityData } from '../../../EntityContext';
import { TestResults } from './TestResults';
<<<<<<< HEAD
import { combineEntityDataWithSiblings, useIsSeparateSiblingsMode } from '../../../siblingUtils';
import { AssertionMonitorBuilderModal } from './assertion/builder/AssertionMonitorBuilderModal';
import { isEntityEligibleForAssertionMonitoring } from './assertion/builder/utils';
import { useAppConfig } from '../../../../../useAppConfig';
=======
import { Assertions } from './Assertions';
import TabToolbar from '../../../components/styled/TabToolbar';
import { useGetValidationsTab } from './useGetValidationsTab';
import { ANTD_GRAY } from '../../../constants';
>>>>>>> oss_master

const TabTitle = styled.span`
    margin-left: 4px;
`;

const TabButton = styled(Button)<{ selected: boolean }>`
    background-color: ${(props) => (props.selected && ANTD_GRAY[3]) || 'none'};
    margin-left: 4px;
`;

enum TabPaths {
    ASSERTIONS = 'Assertions',
    TESTS = 'Tests',
}

const DEFAULT_TAB = TabPaths.ASSERTIONS;

/**
 * Component used for rendering the Entity Validations Tab.
 */
export const ValidationsTab = () => {
<<<<<<< HEAD
    const { urn, entityType, entityData } = useEntityData();
    const { data, refetch } = useGetDatasetAssertionsQuery({ variables: { urn }, fetchPolicy: 'cache-first' });
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const { config } = useAppConfig();
    const assertionMonitorsEnabled = config?.featureFlags?.assertionMonitorsEnabled || false;

    const combinedData = isHideSiblingMode ? data : combineEntityDataWithSiblings(data);
    const [removedUrns, setRemovedUrns] = useState<string[]>([]);
    const [showAssertionBuilder, setShowAssertionBuilder] = useState(false);
    /**
     * Determines which view should be visible: assertions or tests.
     */
    const [view, setView] = useState(ViewType.ASSERTIONS);

    const assertions =
        (combinedData && combinedData.dataset?.assertions?.assertions?.map((assertion) => assertion as Assertion)) ||
        [];
    const filteredAssertions = assertions.filter((assertion) => !removedUrns.includes(assertion.urn));
    const numAssertions = filteredAssertions.length;
=======
    const { entityData } = useEntityData();
    const history = useHistory();
    const { pathname } = useLocation();
>>>>>>> oss_master

    const totalAssertions = (entityData as any)?.assertions?.total;
    const passingTests = (entityData as any)?.testResults?.passing || [];
    const maybeFailingTests = (entityData as any)?.testResults?.failing || [];
    const totalTests = maybeFailingTests.length + passingTests.length;

    const { selectedTab, basePath } = useGetValidationsTab(pathname, Object.values(TabPaths));

    // If no tab was selected, select a default tab.
    useEffect(() => {
        if (!selectedTab) {
            // Route to the default tab.
            history.replace(`${basePath}/${DEFAULT_TAB}`);
        }
    }, [selectedTab, basePath, history]);

    /**
     * The top-level Toolbar tabs to display.
     */
    const tabs = [
        {
            title: (
                <>
                    <FileProtectOutlined />
                    <TabTitle>Assertions ({totalAssertions})</TabTitle>
                </>
            ),
            path: TabPaths.ASSERTIONS,
            disabled: totalAssertions === 0,
            content: <Assertions />,
        },
        {
            title: (
                <>
                    <FileDoneOutlined />
                    <TabTitle>Tests ({totalTests})</TabTitle>
                </>
            ),
            path: TabPaths.TESTS,
            disabled: totalTests === 0,
            content: <TestResults passing={passingTests} failing={maybeFailingTests} />,
        },
    ];

    return (
        <>
            <TabToolbar>
                <div>
<<<<<<< HEAD
                    <Button type="text" onClick={() => setView(ViewType.ASSERTIONS)}>
                        <FileProtectOutlined />
                        Assertions ({numAssertions})
                    </Button>
                    <Button type="text" disabled={totalTests === 0} onClick={() => setView(ViewType.TESTS)}>
                        <FileDoneOutlined />
                        Tests ({totalTests})
                    </Button>
                </div>
            </TabToolbar>
            {(view === ViewType.ASSERTIONS && (
                <>
                    {assertionMonitorsEnabled && isEntityEligibleForAssertionMonitoring(entityData?.platform?.urn) && (
                        <TabToolbar>
                            <Button type="text" onClick={() => setShowAssertionBuilder(true)}>
                                <PlusOutlined /> Create Assertion
                            </Button>
                        </TabToolbar>
                    )}
                    <DatasetAssertionsSummary summary={getAssertionsStatusSummary(filteredAssertions)} />
                    {entityData && (
                        <DatasetAssertionsList
                            assertions={filteredAssertions}
                            onDelete={(assertionUrn) => {
                                // Hack to deal with eventual consistency.
                                setRemovedUrns([...removedUrns, assertionUrn]);
                                setTimeout(() => refetch(), 3000);
                            }}
                        />
                    )}
                </>
            )) || <TestResults passing={passingTests} failing={maybeFailingTests} />}
            {showAssertionBuilder && (
                <AssertionMonitorBuilderModal
                    entityUrn={urn}
                    entityType={entityType}
                    platformUrn={entityData?.platform?.urn as string}
                    onSubmit={() => {
                        setShowAssertionBuilder(false);
                        // TODO: Use the Apollo Cache.
                        setTimeout(() => refetch(), 3000);
                    }}
                    onCancel={() => setShowAssertionBuilder(false)}
                />
            )}
=======
                    {tabs.map((tab) => (
                        <TabButton
                            type="text"
                            disabled={tab.disabled}
                            selected={selectedTab === tab.path}
                            onClick={() => history.replace(`${basePath}/${tab.path}`)}
                        >
                            {tab.title}
                        </TabButton>
                    ))}
                </div>
            </TabToolbar>
            {tabs.filter((tab) => tab.path === selectedTab).map((tab) => tab.content)}
>>>>>>> oss_master
        </>
    );
};
