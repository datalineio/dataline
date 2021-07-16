import React from "react";
import styled from "styled-components";
import { components, IndicatorProps, OptionTypeBase } from "react-select";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faSortDown } from "@fortawesome/free-solid-svg-icons";

const Arrow = styled(FontAwesomeIcon)`
  margin-top: -6px;
`;

const DropdownIndicator: React.FC<IndicatorProps<OptionTypeBase, boolean>> = (
  props
) => {
  return (
    <components.DropdownIndicator {...props}>
      <Arrow icon={faSortDown} />
    </components.DropdownIndicator>
  );
};

export default DropdownIndicator;
