import React from "react";

import { Cell } from "components/SimpleTableComponents";
import { DropDown, DropdownProps } from "components";

const SyncSettingsCell: React.FC<DropdownProps> = (props) => (
  <Cell flex={1.5}>
    <DropDown {...props} fullText withBorder />
  </Cell>
);

export { SyncSettingsCell };
