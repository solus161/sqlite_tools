# https://docs.python.org/3/library/sqlite3.html#sqlite-and-python-types

import re
from datetime import datetime
from typing import TypeVar, Union

from datamodels.exceptions import *
from datamodels.utils import get_class_attrs, get_instance_attrs

# TODO: develop data types for columns


class DataType():
    dtype_name = None           # SQLite data type name, include NULL, INTEGER, REAL, TEXT, BLOB
    dtype = None                # Python data type
    constraints = ['primary_key', 'foreign_key', 'not_null']

    def __init__(self, value=None, **kwargs):
        """
        Return an instance of model

        Params:
         - value: any value accepted, or None
         - foreign_key: list/tuple of [model class, ref_attr_name], or None
        """
        self.primary_key = kwargs.pop('primary_key', False)
        self.foreign_key = kwargs.pop('foreign_key', None)      # sqlite does check for non existing parent id
        self.not_null = kwargs.pop('not_null', False)
        # self.required = kwargs.pop('required', False)
        self.default = kwargs.pop('default', None)
        self._changed = False           # Any change made to the attribute?
        # self._contraint_parser = {
        #     'primary_key': self._parse_primary_key,
        #     'foreign_key': self._parse_foreign_key
        # }

        if self.foreign_key is not None:
            if len(self.foreign_key) != 2:
                raise AttributeForeignKeyError(self.foreign_key)

            parent_attrs = get_class_attrs(self.foreign_key[0])
            parent_key = self.foreign_key[1] if self.foreign_key[1] is not None else 'id'
            if parent_key not in parent_attrs:
                raise AttributeForeignKeyError(self.foreign_key)

        if value is None:
            self.value = self.default
        else:
            self.value = value

    @classmethod
    def get_instance(cls, *args, **kwargs):
        return cls(*args, **kwargs)

    def type_check(self, value):
        if value is None:
            #     if self.required:
            #         raise AttributeRequiredError()
            return True

        if isinstance(value, self.dtype):
            return True
        raise AttributeTypeError(self.dtype, value)

    def autofill_check(self):
        if self.primary_key:
            return True
        return False

    def update(self, value, type_check=True, *args, **kwargs):
        """
        Update current value by new value.
        Return True if new value does not equal to current value.

        Params:
         - value: new value subjected to update
         - type_check: whether checking for valid data type or not
         - overwrite: to conform with DataTypeDateTime
        """

        if self.autofill_check():
            raise AttributeAutofillError(value)

        # type_check == False will lead to bypassing self.type_check
        if not type_check or self.type_check(value):
            if value != self.value:
                self.value = value
                self.set_change()
                return True
            return False

    def set_change(self):
        """
        Change must be made
        """
        self._changed = True

    def clear_change(self):
        self._change = False

    def _overwrite(self, value):
        """
        Just like update, but bypass any autofill protection.
        Used to create new instance from db.
        """
        self.value = value

    def clone(self, value=None, from_db=True):
        """
        Duplicate the instance from model, together with attributes,
        Could assign it a value.
        """
        attrs = get_instance_attrs(self)
        # kwargs = {k: v for k, v in attrs.items()}
        new_instance = self.get_instance(**attrs)
        if value is not None:
            if from_db:
                new_instance._overwrite(value)
            else:
                new_instance.update(value, type_check=False)
        return new_instance

    def autofill(self):
        """
        primary_key is handled by sqlite
        """
        if self.primary_key:
            return

    def check_constraint(self):
        """
        Check whether contrains are satisfied before applying to db
        """
        fn = {
            'primary_key': self._check_primary_key,
            'foreign_key': self._check_foreign_key,
            'not_null': self._check_not_null
            }

        for constraint in self.constraints:
            fn[constraint]()

    def _check_primary_key(self):
        """
        Handled by autofill_check()
        """
        # if self.value is not None and self.primary_key:
        #     raise AttributePrimaryKeyError(self.value)

    def _check_foreign_key(self):
        """
        Handled in __init__()
        """

    def _check_not_null(self):
        if self.not_null:
            if self.value is None:
                raise AttributeRequiredError()

    def to_sqlite(self, value=None) -> Union[str, int, float]:
        """
        Convert to sqlite-compatible value, called when about to be written to db.
        Implemented by each class.
        Constraint must be checked before returning value.
        """
        # self.check_required()
        if value is not None:
            return str(value)

        self.autofill()
        if self.value is not None:
            return str(self.value)
        return 'null'

    def to_json(self):
        """
        Convert some value to str type for using in client.
        """
        return self.value

    def __str__(self):
        return self.value.__str__()


class DataTypeInteger(DataType):
    dtype_name = 'integer'
    dtype = int

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DataTypeFloat(DataType):
    dtype_name = 'real'
    dtype = float

    def __init__(self, value=None, **kwargs):
        super().__init__(value, **kwargs)


class DataTypeFloat(DataType):
    dtype_name = 'real'
    dtype = float

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DataTypeString(DataType):
    dtype_name = 'text'
    dtype = str

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_sqlite(self, value=None) -> str:
        if value is not None:
            return '"' + value + '"'

        if self.value is None:
            return 'null'
        else:
            return '"' + self.value + '"'


class DataTypeDateTime(DataTypeString):
    dtype = datetime
    datetime_pattern = '%Y-%m-%d %H:%M:%S.%f'
    datetime_regex = '\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d{3}'

    def __init__(self, update_on_created=False, update_on_modified=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.value: TypeVar('value', datetime)
        self.update_on_created = update_on_created
        self.update_on_modified = update_on_modified

    def type_check(self, value):
        """
        Check for 1) valid dtype, or 2) valid regex patter
        """
        if isinstance(value, str):
            if re.match(self.datetime_regex, value):
                return True
            return False
        return super().type_check(value)

    def autofill_check(self):
        if self.primary_key:
            return True
        elif self.update_on_modified:
            return True
        elif self.update_on_created and self.value is None:
            return True
        return False

    def update(self, value, type_check=False):
        """
        Accept both datetime type and str type.

        Params:
         - value: either string in datetime_pattern format, or datetime type
         - type_check: similar to parent class
        """
        if self.autofill_check():
            # Update nothing, autofill() will handle the rest
            return

        if not type_check or self.type_check(value):
            if isinstance(value, str):
                value = self._from_str(value)
            return super().update(value, type_check=False)

    def _overwrite(self, value):
        if isinstance(value, str):
            value = self._from_str(value)
        return super()._overwrite(value)

    def _from_str(self, value: str) -> datetime:
        return datetime.strptime(value + '000', self.datetime_pattern)

    def _to_str(self, value: datetime) -> str:
        return '"' + datetime.strftime(value, self.datetime_pattern)[:-3] + '"'    # To milisecs

    def autofill(self):
        """
        Update value for datetime attribute
        """
        if self.update_on_modified:
            self.value = datetime.now()
            self.set_change()
        elif self.update_on_created and self.value is None:
            self.value = datetime.now()
            self.set_change()

    def to_sqlite(self, value=None) -> str:
        if value is not None:
            return self._to_str(value)

        self.autofill()
        if self.value is None:
            return 'null'
        else:
            return self._to_str(self.value)

    def to_json(self):
        return datetime.strftime(self.value, self.datetime_pattern)[:-10]    # No need to display up to seconds


class DataTypeBoolean(DataTypeInteger):
    dtype_name = 'integer'
    dtype = bool

    def update(self, value, type_check=True, *args, **kwargs):
        return super().update(value, type_check, *args, **kwargs)

    def to_sqlite(self, value=None) -> Union[str, int, float]:
        if value is None:
            value = int(self.value)
        return super().to_sqlite(value)
