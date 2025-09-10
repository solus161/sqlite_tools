from typing import ClassVar, Union

from datamodels.datatypes import *
from datamodels.exceptions import *
from datamodels.sqlite_driver import SqliteDriver
from datamodels.sqlparser import *
from datamodels.utils import get_class_attrs, get_instance_attrs


class BasicModel():
    # Add attrs here
    id = DataTypeInteger(primary_key=True)
    name = DataTypeString()

    def __init__(self, *args, **kwargs):
        """
        Do not call this directly as data validation is not done here.
        To instantiate from data, use create_instance() as data validation will be done here

        Params:
         - args, kwargs: attributes' names and values
        """
        # self._id = id               # internal rowid primary key used by sqlite

        # Convert class attrs to instance attrs
        attrs = get_class_attrs(self)
        # print(f'class attrs {attrs.keys()}')
        if kwargs:
            # Values must be validated first in create_instance()
            id = kwargs.pop('id', None)
            for name, attr in attrs.items():
                assert attr is not None     # Model attribute must not be None
                setattr(
                    self, name,
                    attr.clone(
                        value=kwargs.get(name, None),
                        from_db=id is not None))
        elif args:
            id = args[0]
            for name, value in zip(attrs.keys(), args):
                setattr(
                    self, name,
                    attrs[name].clone(
                        value=value,
                        from_db=id is not None))
        else:
            # No attribute arguments provided
            for name, attr in attrs.items():
                assert attr is not None     # Model attribute must not be None
                setattr(self, name, attr.clone())

    def __str__(self) -> str:
        # output = {'id': self._id}
        output = {}
        attrs = get_instance_attrs(self)
        for k, v in attrs.items():
            output[k] = v.value
        return str(output)

    def to_json(self) -> dict:
        """
        Similar to __str__ but some values are converted to str, for sending to client
        """
        output = {}
        attrs = get_instance_attrs(self)
        for k, v in attrs.items():
            output[k] = v.to_json()
        return output

    def get_value(self, attr_name):
        return getattr(self, attr_name).value

    def get_id(self):
        return self.id.value

    @classmethod
    def get_instance_by_id(cls, sqlite_driver: SqliteDriver, id: int, disabled=False):
        """
        Return an instance from db by query for id.

        Params:
         - id: id of the instance
         - disabled: False to not query disabled instance
        """
        attrs = get_class_attrs(cls)
        if 'disabled' in attrs:
            return cls.get_instance(sqlite_driver, {'id': id, 'disabled': disabled})
        else:
            return cls.get_instance(sqlite_driver, {'id': id})

    @classmethod
    def init_table(cls, sqlite_driver: SqliteDriver):
        """
        Create a new table in the connected database.
        """
        command_str = SQLCreateTable.parse(cls)
        sqlite_driver.execute(command_str)
        return True

    @classmethod
    def drop(cls, sqlite_driver: SqliteDriver):
        """
        Delete a table
        """
        command_str = SQLDropTable.parse(cls)
        sqlite_driver.execute(command_str)

    @classmethod
    def delete_rows(cls, sqlite_driver: SqliteDriver, filter=Union[dict, None]):
        """
        Delete a row
        """
        command_str = SQLDeleteInstances.parse(cls, filter)
        sqlite_driver.execute(command_str)
        return True

    @classmethod
    def create(cls):
        """
        Return a model instance
        """

    @classmethod
    def get_instances(cls, sqlite_driver: SqliteDriver, filter=None):
        """
        Return multiple instances from a table
        """
        command_str = SQLGetInstances.parse(cls, filter)
        data = sqlite_driver.execute(command_str, SqliteDriver.FETCH_ALL)
        output = []
        for item in data:
            output.append(cls(*item))
        return output

    @classmethod
    def get_instance(cls, sqlite_driver: SqliteDriver, filter: dict):
        """
        Return an instance match the filter params
        Attr being an foreign key will be returned as an instance
        """
        command_str = SQLGetInstance.parse(cls, filter)
        # print(command_str)
        output = sqlite_driver.execute(command_str, SqliteDriver.FETCH_ONE)
        if output is not None:
            # print(output)
            instance = cls(*output)
            return instance
        return None

    @classmethod
    def create_instance(cls, values: Union[dict, list, tuple]):
        """
        Create a new instance populated by values from attrs.
        If data is from db, id must not be None
        """
        # Validate values first
        if cls.validate(values):
            if isinstance(values, dict):
                return cls(**values)
            elif isinstance(values, list) or isinstance(values, tuple):
                return cls(*values)
        return None

    def migrate(self, sqlite_driver: SqliteDriver):
        """
        Insert the current instance to table if there is no record with the same id exists
        Or create a new one
        """
        if self.id.value is None:
            command_str = SQLInsertInstance.parse(self)
        else:
            command_str = SQLUpdateInstance.parse(self)

        if command_str is None:
            return

        # print(command_str)
        self.check_constraints()
        sqlite_driver.execute(command_str, SqliteDriver.COMMIT)
        self.id.value = sqlite_driver.get_last_insert_rowid() if self.id.value is None else self.id.value
        self.clear_changes()
        return True

    def update(self, values: Union[dict, tuple, list]):
        """
        Update attrs by values provided
        """
        if self.validate(values):
            return self.changes_check(values)

    @classmethod
    def validate(cls, values: Union[dict, list, tuple]) -> bool:
        """
        Validate data before parsing into attributes
        """
        attrs = get_class_attrs(cls)
        if isinstance(values, dict):
            for name, value in values.items():
                if name not in attrs:
                    raise AttributeNotDeclared(name, cls)
                attrs[name].type_check(value)

            # # Not-declared check
            # for name, value in values.items():
            #     if name not in attrs:
            #         raise AttributeNotDeclared(name, cls)

            return True

        elif isinstance(values, list) or isinstance(values, tuple):
            # Check length of values provided
            if len(attrs) != len(values):
                raise AttributesNumberMismatched(cls, attrs, values)

            # Check type
            for k, v in zip(attrs.keys(), values):
                attrs[k].type_check(v)

            return True

    def check_constraints(self):
        """
        Check for contraints satisfied before applying to db.
        """
        attrs = get_instance_attrs(self)
        for name, attr in attrs.items():
            attr.check_constraint()

    def changes_check(self, values) -> list:
        """
        Check changes of attributes' values. Values must be validated first.
        Attributes are updated directly. Attributes having autofill() method will (or kind of) will not be updated.
        The update is handled by attribute itself
        """
        attrs = get_instance_attrs(self)
        changed_attrs = []
        if isinstance(values, dict):
            for name, value in values.items():
                if attrs[name].update(value):
                    changed_attrs.append(name)
        elif isinstance(values, list) or isinstance(values, tuple):
            for name, value in zip(attrs.keys(), values):
                if attrs[name].update(value):
                    changed_attrs.append(name)
        return changed_attrs

    def clear_changes(self):
        for k, v in get_instance_attrs(self).items():
            v.clear_change()

    class __meta__():
        db_name: ClassVar[Union[str, None]] = None

class BasicTestModel(BasicModel):
    text = DataTypeString(not_null=True)
    nbr = DataTypeInteger()
    real = DataTypeFloat()
    timestamp = DataTypeDateTime()
    created = DataTypeDateTime(update_on_created=True)
    modified = DataTypeDateTime(update_on_modified=True)

    class __meta__():
        db_name = 'testmodel'


class SubTestModel(BasicModel):
    text = DataTypeString()
    fk = DataTypeInteger(foreign_key=[BasicTestModel, 'id'], not_null=True)
    not_null_field = DataTypeString(not_null=True)
    created = DataTypeDateTime(update_on_created=True)
    modified = DataTypeDateTime(update_on_modified=True)

    class __meta__():
        db_name = 'subtestmodel'


class SubTestModel1(BasicModel):
    text = DataTypeString()
    # fk = DataTypeInteger(foreign_key = [BasicTestModel, 'hehe'], not_null = True)
    created = DataTypeDateTime(update_on_created=True)
    modified = DataTypeDateTime(update_on_modified=True)

    class __meta__():
        db_name = 'subtestmodel1'


class DatasetType(BasicModel):
    name = DataTypeString(not_null=True)


class ClassSets(BasicModel):
    name = DataTypeString()

    class __meta__():
        db_name = 'class_sets'


class Classes(BasicModel):
    class_set_id = DataTypeInteger(foreign_key=[ClassSets, None])
    name = DataTypeString(not_null=True)
    plc_id = DataTypeInteger(not_null=True)

    class __meta__():
        db_name = 'classes'


class Datasets(BasicModel):
    name = DataTypeString(not_null=True)
    desc = DataTypeString()
    class_set_id = DataTypeInteger(foreign_key=[ClassSets, None])
    type_id = DataTypeInteger(foreign_key=[DatasetType, None])
    # img_nbr = DataTypeInteger(default = 0)
    # annotated_nbr = DataTypeInteger(default = 0)
    # segmented_nbr = DataTypeInteger(default = 0)
    # object_extracted_nbr = DataTypeInteger(default = 0)
    path = DataTypeString(default='')
    readonly = DataTypeBoolean(not_null=True, default=False)
    disabled = DataTypeBoolean(default=False)
    datetime_created = DataTypeDateTime(update_on_created=True)
    datetime_modified = DataTypeDateTime(update_on_modified=True)

    class __meta__():
        db_name = 'datasets'


class ImagesRaw(BasicModel):
    dataset_id = DataTypeInteger(foreign_key=[Datasets, None])
    class_id = DataTypeInteger(foreign_key=[Classes, None])
    name = DataTypeString(not_null=True)
    annotated = DataTypeBoolean(default=False)
    segmented = DataTypeBoolean(default=False)
    extracted = DataTypeBoolean(default=False)
    datetime_created = DataTypeDateTime(update_on_created=True)
    datetime_modified = DataTypeDateTime(update_on_modified=True)

    class __meta__():
        db_name = 'images_raw'


class DatasetHistory(BasicModel):
    dataset_id = DataTypeInteger(foreign_key=[Datasets, None])
    timestamp = DataTypeDateTime(update_on_created=True)
    action = DataTypeString()

    class __meta__():
        db_name = 'datasets_history'


class Tasks(BasicModel):
    name = DataTypeString(not_null=True)
    desc = DataTypeString()

    class __meta__():
        db_name = 'tasks'


class Models(BasicModel):
    name = DataTypeString(not_null=True)
    desc = DataTypeString()
    class_set_id = DataTypeInteger(foreign_key=[ClassSets, None])
    task_id = DataTypeInteger(foreign_key=[Tasks, None])
    version_nbr = DataTypeInteger(default=0)
    readonly = DataTypeBoolean(not_null=True, default=False)
    disabled = DataTypeBoolean(default=False)
    datetime_created = DataTypeDateTime(update_on_created=True)
    datetime_modified = DataTypeDateTime(update_on_modified=True)

    class __meta__():
        db_name = 'models'


class ModelVersions(BasicModel):
    model_id = DataTypeInteger(foreign_key=[Models, None])
    desc = DataTypeString()
    path = DataTypeString()                                         # folder unique name
    path_train = DataTypeString()                                   # <unique_name>/.path file
    path_infer = DataTypeString()                                   # <unique_name>/.rt file
    pretrained_id = DataTypeInteger()
    disabled = DataTypeBoolean(default=False)
    datetime_created = DataTypeDateTime(update_on_created=True)
    datetime_modified = DataTypeDateTime(update_on_modified=True)

    class __meta__():
        db_name = 'model_versions'


class TrainHistory(BasicModel):
    version_id = DataTypeInteger(foreign_key=[ModelVersions, None])
    desc = DataTypeString()
    train_id = DataTypeInteger(foreign_key=[Datasets, None])
    valid_id = DataTypeInteger(foreign_key=[Datasets, None])
    epochs = DataTypeInteger()
    loss = DataTypeFloat()
    datetime_created = DataTypeDateTime(update_on_created=True)
    datetime_modified = DataTypeDateTime(update_on_modified=True)

    class __meta__():
        db_name = 'train_history'


class TestHistory(BasicModel):
    version_id = DataTypeInteger(foreign_key=[ModelVersions, None])
    desc = DataTypeString()
    test_id = DataTypeInteger(foreign_key=[Datasets, None])
    score = DataTypeFloat()
    datetime_created = DataTypeDateTime(update_on_created=True)
    datetime_modified = DataTypeDateTime(update_on_modified=True)

    class __meta__():
        db_name = 'test_history'
