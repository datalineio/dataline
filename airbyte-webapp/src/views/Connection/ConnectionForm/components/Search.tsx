import React from "react";
import { useIntl } from "react-intl";
import styled from "styled-components";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faSearch } from "@fortawesome/free-solid-svg-icons";

import { Input } from "components";

type SearchProps = {
  onSearch: (value: string) => void;
};

const SearchInput = styled(Input)`
  padding: 3px 10px 3px 24px;
`;

const SearchIcon = styled(FontAwesomeIcon)`
  position: absolute;
  top: 6px;
  left: 6px;
  font-size: 14px;
  color: ${({ theme }) => theme.greyColor40};
`;

const SearchContent = styled.div`
  position: relative;
  max-width: 270px;
  width: 100%;

  &:before {
    content: attr(data-content);
  }
`;

const Search: React.FC<SearchProps> = ({ onSearch }) => {
  const formatMessage = useIntl().formatMessage;

  return (
    <SearchContent>
      <SearchIcon icon={faSearch} />
      <SearchInput
        placeholder={formatMessage({
          id: `form.nameSearch`,
        })}
        onChange={(e) => onSearch(e.target.value)}
      />
    </SearchContent>
  );
};

export default Search;
