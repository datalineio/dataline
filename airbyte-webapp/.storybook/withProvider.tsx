import { Router } from "react-router-dom";
import * as React from "react";
import { createMemoryHistory } from "history";
import { ThemeProvider } from "styled-components";

import { Theme } from "../src/theme";

interface Props {
  theme?: Theme;
}

interface Props {
  children?: React.ReactNode;
  theme?: Theme;
}

class WithProviders extends React.Component<Props> {
  render() {
    const { theme, children } = this.props;

    return (
      <Router history={createMemoryHistory()}>
        <ThemeProvider theme={theme}>{children}</ThemeProvider>
      </Router>
    );
  }
}

export default WithProviders;
