import * as React from 'react';
import { NavLink, useLocation } from 'react-router-dom';
import {
  Button,
  Masthead,
  MastheadBrand,
  MastheadLogo,
  MastheadMain,
  MastheadToggle,
  Nav,
  NavExpandable,
  NavItem,
  NavList,
  Page,
  PageSidebar,
  PageSidebarBody,
  SkipToContent,
} from '@patternfly/react-core';
import { IAppRoute, IAppRouteGroup, routes } from '@app/routes';
import { BarsIcon } from '@patternfly/react-icons';

interface IAppLayout {
  children: React.ReactNode;
}

const AppLayout: React.FunctionComponent<IAppLayout> = ({ children }) => {
  const [sidebarOpen, setSidebarOpen] = React.useState(true);
  const masthead = (
    <Masthead>
      <MastheadMain>
        <MastheadToggle>
          <Button
            icon={<BarsIcon />}
            variant="plain"
            onClick={() => setSidebarOpen(!sidebarOpen)}
            aria-label="Global navigation"
          />
        </MastheadToggle>
        <MastheadBrand data-codemods>
          <MastheadLogo data-codemods>
            <svg height="40px" viewBox="0 0 679 158">

              <title>PatternFly</title>
              <defs>
                <linearGradient id="linearGradient-website-masthead" x1="68%" x2="32%" y1="2.25860997e-13%" y2="100%">
                  <stop offset="0%" stopColor="#2B9AF3" />
                  <stop offset="100%" stopColor="#73BCF7" stopOpacity="0.5" />
                </linearGradient>
              </defs>
              <g className="layer">
                <title>Layer 1</title>
                <g fill="var(--pf-t--global--text--color--regular)" fillRule="nonzero" id="svg_2">
                  <text fill="#000000" fontFamily="Helvetica" fontSize="24" fontWeight="bold" id="svg_17" strokeWidth="0" textAnchor="middle" transform="matrix(2.09015 0 0 2.86039 -4.70208 76.7012)" x="213" xmlSpace="preserve" y="7">Extração Inteligente</text>
                </g>
                <g id="svg_13">
                  <path d="m61.83,0l96.17,0l0,96.17l-10.3,0c-47.43,0 -85.87,-38.44 -85.87,-85.87l0,-10.3l0,0z" fill="var(--pf-t--color--blue--50)" id="svg_14" />
                  <path d="m158,3.43l-92.74,154.57l72.74,0c11.05,0 20,-8.95 20,-20l0,-134.57l0,0z" fill="url(#linearGradient-website-masthead)" id="svg_15" />
                  <path d="m123.65,-30.91l-92.74,154.56l72.74,0c11.05,0 20,-8.95 20,-20l0,-134.56l0,0z" fill="url(#linearGradient-website-masthead)" id="svg_16" transform="translate(77.282609, 46.369565) scale(1, -1) rotate(90.000000) translate(-77.282609, -46.369565) " />
                </g>
              </g>
            </svg>
          </MastheadLogo>
        </MastheadBrand>
      </MastheadMain>
    </Masthead>
  );

  const location = useLocation();

  const renderNavItem = (route: IAppRoute, index: number) => (
    <NavItem key={`${route.label}-${index}`} id={`${route.label}-${index}`} isActive={route.path === location.pathname}>
      <NavLink
        to={route.path}
      >
        {route.label}
      </NavLink>
    </NavItem>
  );

  const renderNavGroup = (group: IAppRouteGroup, groupIndex: number) => (
    <NavExpandable
      key={`${group.label}-${groupIndex}`}
      id={`${group.label}-${groupIndex}`}
      title={group.label}
      isActive={group.routes.some((route) => route.path === location.pathname)}
    >
      {group.routes.map((route, idx) => route.label && renderNavItem(route, idx))}
    </NavExpandable>
  );

  const Navigation = (
    <Nav id="nav-primary-simple">
      <NavList id="nav-list-simple">
        {routes.map(
          (route, idx) => route.label && (!route.routes ? renderNavItem(route, idx) : renderNavGroup(route, idx)),
        )}
      </NavList>
    </Nav>
  );

  const Sidebar = (
    <PageSidebar>
      <PageSidebarBody>{Navigation}</PageSidebarBody>
    </PageSidebar>
  );

  const pageId = 'primary-app-container';

  const PageSkipToContent = (
    <SkipToContent
      onClick={(event) => {
        event.preventDefault();
        const primaryContentContainer = document.getElementById(pageId);
        primaryContentContainer?.focus();
      }}
      href={`#${pageId}`}
    >
      Skip to Content
    </SkipToContent>
  );
  return (
    <Page
      mainContainerId={pageId}
      masthead={masthead}
      sidebar={sidebarOpen && Sidebar}
      skipToContent={PageSkipToContent}
    >
      {children}
    </Page>
  );
};

export { AppLayout };
