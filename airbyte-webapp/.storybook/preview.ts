import { addDecorator } from "@storybook/react";
import { withThemesProvider } from "storybook-addon-styled-component-theme";

import WithProviders from "./withProvider";
import { theme } from "../src/theme";

addDecorator(withThemesProvider([theme], WithProviders));

export const parameters = {};
