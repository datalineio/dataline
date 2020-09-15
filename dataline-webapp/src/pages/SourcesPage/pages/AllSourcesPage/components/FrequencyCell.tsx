import React from "react";
import styled from "styled-components";

import FrequencyConfig from "../../../../../data/FrequencyConfig.json";

type IProps = {
  value: { units: number; timeUnit: string };
  enabled?: boolean;
};

const Content = styled.div<{ enabled?: boolean }>`
  color: ${({ theme, enabled }) => (!enabled ? theme.greyColor40 : "inheret")};
`;

const FrequencyCell: React.FC<IProps> = ({ value, enabled }) => {
  const cellText = FrequencyConfig.find(
    item => JSON.stringify(item.config) === JSON.stringify(value)
  );
  return <Content enabled={enabled}>{cellText?.text || ""}</Content>;
};

export default FrequencyCell;
