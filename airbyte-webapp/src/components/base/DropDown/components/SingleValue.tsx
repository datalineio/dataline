import React from "react";
import styled from "styled-components";
import { components, SingleValueProps, OptionTypeBase } from "react-select";

import { IDataItem } from "./Option";
import Text from "./Text";

export type IProps = {
  data?: IDataItem;
} & SingleValueProps<OptionTypeBase>;

const ItemView = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: left;
  align-items: center;
`;

// const GroupTitle = styled(Text)`
//   text-transform: capitalize;
//   font-size: 14px;
//   line-height: 13px;
// `;
//
// const GroupText = styled.div`
//   color: ${({ theme }) => theme.greyColor40};
//   font-size: 12px;
//   line-height: 11px;
// `;

const Icon = styled.div`
  margin-right: 6px;
  display: inline-block;
`;

const SingleValue: React.FC<IProps> = (props) => {
  // if (item.groupValue || item.groupValueText) {
  //   return (
  //     <div>
  //       <GroupTitle>{item.groupValue || item.groupValueText}</GroupTitle>
  //       <GroupText>{item.text}</GroupText>
  //     </div>
  //   );
  // }

  return (
    <ItemView>
      {props.data.img ? <Icon>{props.data.img}</Icon> : null}
      <Text>
        <components.SingleValue {...props}>
          {props.children}
        </components.SingleValue>
      </Text>
    </ItemView>
  );
};

export default React.memo(SingleValue);
