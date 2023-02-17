import { useEffect } from 'react';
import { useHistory, useLocation } from 'react-router';
import { PageRoutes } from '../../conf/Global';

export function useInitialRedirect(state, localState, setState, setLocalState) {
    const location = useLocation();
    const history = useHistory();

    /**
     * Route to the most recently visited path once on first load of home page, if present in local storage
     * and if it is not an embedded page.
     *
     */
    useEffect(() => {
        if (
            location.pathname === PageRoutes.ROOT &&
            !state.loadedInitialPath &&
            localState.selectedPath !== location.pathname
        ) {
            setState({
                ...state,
                loadedInitialPath: true,
            });
            if (localState.selectedPath && !localState.selectedPath.includes(PageRoutes.EMBED)) {
                history.push({
                    pathname: localState.selectedPath,
                    search: localState.selectedSearch || '',
                });
            }
        }
    }, [
        localState.selectedPath,
        localState.selectedSearch,
        location.pathname,
        location.search,
        state,
        history,
        setState,
    ]);

    /**
     * When the location of the browse changes, save the latest to local state if it's not an embedded page.
     */
    useEffect(() => {
        if (
            !location.pathname.includes(PageRoutes.EMBED) &&
            (localState.selectedPath !== location.pathname || localState.selectedSearch !== location.search)
        ) {
            setLocalState({
                ...localState,
                selectedPath: location.pathname,
                selectedSearch: location.search,
            });
        }
    }, [location.pathname, location.search, localState, setLocalState]);
}
