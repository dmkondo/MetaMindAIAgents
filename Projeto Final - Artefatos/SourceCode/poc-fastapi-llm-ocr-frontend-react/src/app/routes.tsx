import * as React from 'react';
import { Route, Routes } from 'react-router-dom';
import { Dashboard } from '@app/Dashboard/Dashboard';
import { GeneralSettings } from '@app/Settings/General/GeneralSettings';
import { ProfileSettings } from '@app/Settings/Profile/ProfileSettings';
import { NotFound } from '@app/NotFound/NotFound';

export interface IAppRoute {
  label?: string; // Excluding the label will exclude the route from the nav sidebar in AppLayout
  /* eslint-disable @typescript-eslint/no-explicit-any */
  element: React.ReactElement;
  /* eslint-enable @typescript-eslint/no-explicit-any */
  exact?: boolean;
  path: string;
  title: string;
  routes?: undefined;
}

export interface IAppRouteGroup {
  label: string;
  routes: IAppRoute[];
}

export type AppRouteConfig = IAppRoute | IAppRouteGroup;

const routes: AppRouteConfig[] = [
  {
    element: <Dashboard />,
    exact: true,
    label: 'Cadastro de Notas Fiscais',
    path: '/',
    title: 'Extração Inteligente | Cadastro de Notas Fiscais',
  },

 
      {
        element: <GeneralSettings />,
        exact: true,
        label: 'Prompt de Extração',
        path: '/settings/general',
        title: 'Extração Inteligente | Prompt',
      },
      {
        element: <ProfileSettings />,
        exact: true,
        label: 'Selecionar LLM',
        path: '/settings/profile',
        title: 'Extração Inteligente | Selecionar LLM',
      },
 

  {
    element: <NotFound />,
    exact: true,
    label: 'Sobre',
    path: '/sobre',
    title: 'Extração Inteligente | Sobre',
  },
];

const flattenedRoutes: IAppRoute[] = routes.reduce(
  (flattened, route) => [...flattened, ...(route.routes ? route.routes : [route])],
  [] as IAppRoute[],
);

const AppRoutes = (): React.ReactElement => (
  <Routes>
    {flattenedRoutes.map(({ path, element }, idx) => (
      <Route path={path} element={element} key={idx} />
    ))}
    <Route element={<NotFound />} />
  </Routes>
);

export { AppRoutes, routes };
