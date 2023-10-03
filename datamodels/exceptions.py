"""
Customed exceptions for models
"""


class AttributeRequiredError(Exception):
    """
    Raised when a required attribute is provided no value, or None
    """

    def __init__(self):
        """
        Params:
         - provided_value: value assigned to the attribute, if any
        """
        self.message = f'Attribute is required'
        super().__init__(self.message)


class AttributeTypeError(Exception):
    """
    Raised when an attribute got assigned a value of wrong type
    """

    def __init__(self, required_dtype, provided_value):
        """
        Params:
         - required_dtype: the required type, output of type()
         - provided_value: value assigned to the attribute, if any
        """
        self.message = f'Attribute requires data type of {required_dtype}, but value {provided_value} of type {type(provided_value)} provided instead'
        super().__init__(self.message)


class AttributeNotDeclared(Exception):
    """
    Raised when an attribute it not declared in model but got assigned a value
    """

    def __init__(self, attr_name: str, cls):
        """
        Params:
         - attr_name: name of the related attribute
         - cls: class object of the model
        """
        self.message = f'Attribute "{attr_name}" is not declared in class {cls}'
        super().__init__(self.message)


class AttributesNumberMismatched(Exception):
    """
    Raised when the number of attributes do not equal to the number of values provided
    """

    def __init__(self, cls, attrs, values):
        """
        Params:
         - cls: class object of the model
         - attrs: dict of class attributes
         - values: list or tuple of provided values
        """
        self.message = f'The model {cls} has {len(attrs)} attributes while {len(values)} values are provided.'
        super().__init__(self.message)


class AttributePrimaryKeyError(Exception):
    """
    Raised when an attribute with primary key having not None value
    """

    def __init__(self, value):
        self.message = f'Attribute identified as primary key must have value as None, got {value} instead'
        super().__init__(self.message)


class AttributeForeignKeyError(Exception):
    """
    Raised when foreign key attribute is failed to declare
    """

    def __init__(self, value):
        self.message = f'Foreign key attribute must: 1) be a pair of object, and 2) parent and parent key must exist. Got {value} instead'
        super().__init__(self.message)


class AttributeAutofillError(Exception):
    """
    Raised when an attribute with autofill properties got update()
    """

    def __init__(self, value):
        self.message = f'Attribute value must not be updated by user. Got assigned value {value}'
        super().__init__(self.message)
