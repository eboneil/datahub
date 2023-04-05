import { orange } from '@ant-design/colors';
import { WarningFilled } from '@ant-design/icons';
import React from 'react';
import { useParams } from 'react-router';
import styled from 'styled-components';
import { useSearchAcrossLineageQuery } from '../../../../graphql/search.generated';
import { HAS_ACTIVE_INCIDENTS_FILTER_NAME, HAS_FAILING_ASSERTIONS_FILTER_NAME } from '../../../search/utils/constants';
import { useAppConfig } from '../../../useAppConfig';
import { decodeUrn } from '../utils';
import { generateQueryVariables } from './UpstreamHealth/utils';

// Do not update unless you update the reference to this ID in our Chrome extension code
const ICON_ID = 'embedded-datahub-health-icon';

const StyledWarning = styled(WarningFilled)`
    color: ${orange[5]};
    font-size: 12px;
`;

interface RouteParams {
    urn: string;
}

// Used by our Chrome Extension to show a warning health icon if there are unhealthy upstreams
export default function EmbeddedHealthIcon() {
    const { urn: encodedUrn } = useParams<RouteParams>();
    const urn = decodeUrn(encodedUrn);
    const appConfig = useAppConfig();
    const lineageEnabled: boolean = appConfig?.config?.chromeExtensionConfig?.lineageEnabled || false;

    const { data: incidentsData } = useSearchAcrossLineageQuery(
        generateQueryVariables(urn, HAS_ACTIVE_INCIDENTS_FILTER_NAME, 0, false, true, !lineageEnabled, 0),
    );

    const { data: assertionsData } = useSearchAcrossLineageQuery(
        generateQueryVariables(urn, HAS_FAILING_ASSERTIONS_FILTER_NAME, 0, true, false, !lineageEnabled, 0),
    );

    if (incidentsData?.searchAcrossLineage?.total || assertionsData?.searchAcrossLineage?.total) {
        return <StyledWarning id={ICON_ID} />;
    }

    return null;
}
