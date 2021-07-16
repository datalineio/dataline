import React, { ReactNode, useState } from "react";
import { DropDown } from "components";
import { DropdownProps } from "../DropDown";

const D = ({ children, isOpen, target, onClose }: any) => (
  <div>
    {target}
    {isOpen ? children : null}
    {isOpen ? <div onClick={onClose} /> : null}
  </div>
);

type PopoutProps = DropdownProps & {
  targetComponent: (props: { onOpen: () => void }) => ReactNode;
  onChange: any;
};

const selectStyles = {
  control: (provided: any) => ({ ...provided, minWidth: 240, marginTop: 8 }),
};

const Popout: React.FC<PopoutProps> = ({
  onChange,
  targetComponent,
  ...props
}) => {
  const [state, setState] = useState({ isOpen: false, value: undefined });
  const toggleOpen = () => {
    setState((prevState) => ({ ...prevState, isOpen: !prevState.isOpen }));
  };
  const onSelectChange = (value: any) => {
    toggleOpen();
    onChange(value);
  };

  const { isOpen, value } = state;
  return (
    <D
      isOpen={isOpen}
      onClose={toggleOpen}
      target={targetComponent({ onOpen: toggleOpen })}
    >
      <DropDown
        autoFocus
        backspaceRemovesValue={false}
        components={{
          IndicatorSeparator: null,
          DropdownIndicator: null,
        }}
        controlShouldRenderValue={false}
        hideSelectedOptions={false}
        isClearable={false}
        menuIsOpen
        onChange={onSelectChange}
        options={props.options}
        placeholder={null}
        styles={selectStyles}
        tabSelectsValue={false}
        value={value}
      />
    </D>
  );
};

export { Popout };
