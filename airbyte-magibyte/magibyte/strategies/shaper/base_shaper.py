from magibyte.strategies.base_operation import BaseOperation


class BaseSelect(BaseOperation):
    def shape(self, context):
        raise NotImplementedError()
