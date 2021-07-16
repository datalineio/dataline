import React, { Suspense, useEffect } from "react";
import {
  BrowserRouter as Router,
  Redirect,
  Route,
  Switch,
} from "react-router-dom";
import { useIntl } from "react-intl";

import config from "config";

import SourcesPage from "./SourcesPage";
import DestinationPage from "./DestinationPage";
import PreferencesPage from "./PreferencesPage";
import OnboardingPage from "./OnboardingPage";
import ConnectionPage from "./ConnectionPage";
import SettingsPage from "./SettingsPage";
import LoadingPage from "components/LoadingPage";
import MainView from "components/MainView";
import SupportChat from "components/SupportChat";

import useSegment from "components/hooks/useSegment";
import useFullStory from "components/hooks/useFullStory";
import useRouter from "components/hooks/useRouterHook";
import useWorkspace from "components/hooks/services/useWorkspaceHook";
import { AnalyticsService } from "core/analytics/AnalyticsService";
import { useNotificationService } from "components/hooks/services/Notification/NotificationService";
import { useApiHealthPoll } from "components/hooks/services/Health";
import useOpenReplay from "../components/hooks/useOpenReplay";

export enum Routes {
  Preferences = "/preferences",
  Onboarding = "/onboarding",

  Connections = "/connections",
  Destination = "/destination",
  Source = "/source",
  Connection = "/connection",
  ConnectionNew = "/new-connection",
  SourceNew = "/new-source",
  DestinationNew = "/new-destination",
  Settings = "/settings",
  Configuration = "/configuration",
  Notifications = "/notifications",
  Metrics = "/metrics",
  Account = "/account",
  Root = "/",
}

const getPageName = (pathname: string) => {
  const itemSourcePageRegex = new RegExp(`${Routes.Source}/.*`);
  const itemDestinationPageRegex = new RegExp(`${Routes.Destination}/.*`);
  const itemSourceToDestinationPageRegex = new RegExp(
    `(${Routes.Source}|${Routes.Destination})${Routes.Connection}/.*`
  );

  if (pathname === Routes.Destination) {
    return "Destinations Page";
  }
  if (pathname === Routes.Root) {
    return "Sources Page";
  }
  if (pathname === `${Routes.Source}${Routes.SourceNew}`) {
    return "Create Source Page";
  }
  if (pathname === `${Routes.Destination}${Routes.DestinationNew}`) {
    return "Create Destination Page";
  }
  if (
    pathname === `${Routes.Source}${Routes.ConnectionNew}` ||
    pathname === `${Routes.Destination}${Routes.ConnectionNew}`
  ) {
    return "Create Connection Page";
  }
  if (pathname.match(itemSourceToDestinationPageRegex)) {
    return "Source to Destination Page";
  }
  if (pathname.match(itemDestinationPageRegex)) {
    return "Destination Item Page";
  }
  if (pathname.match(itemSourcePageRegex)) {
    return "Source Item Page";
  }
  if (pathname === `${Routes.Settings}${Routes.Source}`) {
    return "Settings Sources Connectors Page";
  }
  if (pathname === `${Routes.Settings}${Routes.Destination}`) {
    return "Settings Destinations Connectors Page";
  }
  if (pathname === `${Routes.Settings}${Routes.Configuration}`) {
    return "Settings Configuration Page";
  }
  if (pathname === `${Routes.Settings}${Routes.Notifications}`) {
    return "Settings Notifications Page";
  }
  if (pathname === `${Routes.Settings}${Routes.Metrics}`) {
    return "Settings Metrics Page";
  }
  if (pathname === Routes.Connections) {
    return "Connections Page";
  }

  return "";
};

const MainViewRoutes = () => {
  const { pathname } = useRouter();
  useEffect(() => {
    const pageName = getPageName(pathname);
    if (pageName) {
      AnalyticsService.page(pageName);
    }
  }, [pathname]);

  return (
    <MainView>
      <Suspense fallback={<LoadingPage />}>
        <Switch>
          <Route path={Routes.Destination}>
            <DestinationPage />
          </Route>
          <Route path={Routes.Source}>
            <SourcesPage />
          </Route>
          <Route path={Routes.Connections}>
            <ConnectionPage />
          </Route>
          <Route path={Routes.Settings}>
            <SettingsPage />
          </Route>
          <Route exact path={Routes.Root}>
            <SourcesPage />
          </Route>
          <Redirect to={Routes.Root} />
        </Switch>
      </Suspense>
    </MainView>
  );
};

const PreferencesRoutes = () => {
  return (
    <Switch>
      <Route path={Routes.Preferences}>
        <PreferencesPage />
      </Route>
      <Redirect to={Routes.Preferences} />
    </Switch>
  );
};

const OnboardingsRoutes = () => {
  return (
    <Switch>
      <Route path={Routes.Onboarding}>
        <OnboardingPage />
      </Route>
      <Redirect to={Routes.Onboarding} />
    </Switch>
  );
};

export const Routing: React.FC = () => {
  useApiHealthPoll(config.healthCheckInterval);
  useSegment(config.segment.token);
  useFullStory(config.fullstory);
  const tracker = useOpenReplay(config.openreplay.projectKey);

  const { workspace } = useWorkspace();

  useEffect(() => {
    if (workspace) {
      AnalyticsService.identify(workspace.customerId);
      tracker.setUserID(workspace.customerId);
    }
  }, [workspace]);

  const { formatMessage } = useIntl();
  useNotificationService(
    config.isDemo
      ? {
          id: "demo.message",
          title: formatMessage({ id: "demo.message.title" }),
          text: formatMessage({ id: "demo.message.body" }),
          nonClosable: true,
        }
      : undefined
  );

  return (
    <Router>
      <Suspense fallback={<LoadingPage />}>
        {!workspace.initialSetupComplete ? (
          <PreferencesRoutes />
        ) : workspace.displaySetupWizard ? (
          <OnboardingsRoutes />
        ) : (
          <MainViewRoutes />
        )}
        <SupportChat
          papercupsConfig={config.papercups}
          customerId={workspace.customerId}
          onClick={() => window.open(config.ui.slackLink, "_blank")}
        />
      </Suspense>
    </Router>
  );
};
