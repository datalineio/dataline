import styled from "styled-components";

const FormPageContent = styled.div<{ big?: boolean }>`
  max-width: ${({ big }) => (big ? 1279 : 813)}px;
  margin: 13px auto;
`;

export default FormPageContent;
