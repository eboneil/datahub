import { useMemo } from 'react';
import { QueryResult } from '@apollo/client';
import { EntityType, Exact, SearchMultipleInput } from '../../types.generated';
import { GetSearchResultsQuery, useGetSearchResultsQuery } from '../../graphql/search.generated';

type AllEntityInput<T, K> = Pick<T, Exclude<keyof T, keyof K>> & K;

export function useGetEntitySearchResults(
    input: AllEntityInput<SearchMultipleInput, { type?: EntityType }>,
    searchTypes: Array<EntityType>,
) {
    const result: {
        [key in EntityType]: QueryResult<
            GetSearchResultsQuery,
            Exact<{
                input: SearchMultipleInput;
            }>
        >;
    } = {} as {
        [key in EntityType]: QueryResult<
            GetSearchResultsQuery,
            Exact<{
                input: SearchMultipleInput;
            }>
        >;
    };
    for (let i = 0; i < searchTypes.length; i++) {
        const type = searchTypes[i];
        // eslint-disable-next-line react-hooks/rules-of-hooks
        result[type] = useGetSearchResultsQuery({
            variables: {
                input: {
                    type,
                    ...input,
                },
            },
        });
    }

    return useMemo(
        () => result,
        // eslint-disable-next-line react-hooks/exhaustive-deps
        Object.keys(result).map((key) => result[key]),
    );
}
