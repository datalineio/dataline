import React from "react";
import styled from "styled-components";
import { components, OptionProps, OptionTypeBase } from "react-select";
import { theme } from "theme";

import { naturalComparatorBy } from "utils/objects";
import Text from "./Text";
import CheckBox from "components/base/CheckBox";

export type IProps = {
  data: { disabled: boolean; index: number; fullText?: boolean } & IDataItem;
} & OptionProps<OptionTypeBase, boolean>;

export type IDataItem = {
  label?: string;
  value?: any;
  groupValue?: string;
  groupValueText?: string;
  img?: React.ReactNode;
  primary?: boolean;
  secondary?: boolean;
  config?: any;
};

const ItemView = styled.div<{ isSelected?: boolean }>`
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  align-items: center;
  cursor: pointer;
  color: ${({ isSelected }) =>
    isSelected ? theme.primaryColor : theme.textColor};
  background: ${({ isSelected }) =>
    isSelected ? theme.primaryColor12 : theme.whiteColor};
  border: none;
  padding: 10px 16px;
  font-size: 14px;
  line-height: 19px;

  &:hover {
    background: ${({ isSelected }) =>
      isSelected ? theme.primaryColor12 : theme.greyColor0};
  }
`;

const Option: React.FC<IProps> = (props) => {
  return (
    <components.Option {...props}>
      <ItemView
        data-id={props.data.value}
        isSelected={props.isSelected && !props.isMulti}
      >
        <Text
          primary={props.data.primary}
          secondary={props.data.secondary}
          fullText={props.data.fullText}
        >
          {props.isMulti && (
            <>
              <CheckBox checked={props.isSelected} />{" "}
            </>
          )}
          {props.label}
        </Text>
        {props.data.img || null}
      </ItemView>
    </components.Option>
  );
};

export const defaultDataItemSort = naturalComparatorBy<IDataItem>(
  (dataItem) => dataItem.label || ""
);

export default Option;
