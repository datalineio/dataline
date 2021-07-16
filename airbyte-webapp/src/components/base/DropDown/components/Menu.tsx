import React from "react";
import { components, MenuProps } from "react-select";
import styled from "styled-components";
import { theme } from "theme";
import { IDataItem } from "./Option";

// function getLength(options) {
//   return options.reduce((acc, curr) => {
//     if (curr.options) return acc + getLength(curr.options);
//     return acc + 1;
//   }, 0);
// }

// const menuHeaderStyle = {
//   padding: "8px 12px",
// };

// const Menu = (props) => {
//   const optionsLength = getLength(props.options);
//   return (
//     <Fragment>
//       <div style={menuHeaderStyle}>
//         Custom Menu with {optionsLength} options
//       </div>
//       <components.Menu {...props}>{props.children}</components.Menu>
//     </Fragment>
//   );
// };

const MenuList = styled(components.Menu)`
  background: ${theme.textColor};
  background: red;

  &.react-select__menu {
    margin: 0;
    min-width: 260px;
    border-radius: 4px;
    background: ${({ theme }) => theme.whiteColor};
    box-shadow: 0 8px 10px 0 rgba(11, 10, 26, 0.04),
      0 3px 14px 0 rgba(11, 10, 26, 0.08), 0 5px 5px 0 rgba(11, 10, 26, 0.12);

    & .react-select__option {
      padding: 0;
      margin: 0;
      background: ${({ theme }) => theme.whiteColor};

      &.react-select__option--is-selected,
      &.react-select__option--is-focused {
        background: rgba(255, 255, 255, 0);
      }
    }
  }
`;

const Menu: React.FC<MenuProps<IDataItem, boolean>> = (props) => {
  return <MenuList {...props}>{props.children}</MenuList>;
};

export default Menu;
