import re
from typing import Union

from datamodels.utils import (get_class_attrs, get_instance_attrs,
                              get_table_name)


class SQLParser():
    @classmethod
    def parse(cls, table) -> Union[str, dict]:
        return get_table_name(table), get_class_attrs(table)

    @classmethod
    def parse_filter(cls, attrs: dict, filter: dict):
        """
        Convert dict to conditional sql string.
        Support equal operation only, at the moment.
        """
        filter_str = []
        for name, value in filter.items():
            if value is None:
                continue

            # if name == 'id':
            #     filter_str.append(f'{name} = {value}')
            # else:
            filter_str.append(f'{name} = {attrs[name].to_sqlite(value)}')
        return ' and '.join(filter_str)

    @classmethod
    def reformat(cls, text: str):
        return re.sub('\s+', ' ', text).strip()


class SQLCreateTable(SQLParser):
    command = 'create table if not exists {table_name} ({cols_str})'
    # foreign_key_command = 'foreign key ({}) references {}'

    @classmethod
    def parse(cls, table) -> str:
        """
        Parse a create table sql string
        """
        table_name, attrs = super().parse(table)
        data = {'table_name': table_name, 'cols_str': None}
        cols_str = []

        for name, attr in attrs.items():
            cols_str.append(cls._parse_col(name, attr))
        data['cols_str'] = cls.reformat(', '.join(cols_str))
        return cls.reformat(cls.command.format(**data))

    @classmethod
    def _parse_col(cls, name: str, col) -> str:
        col_str = '{col_name} {type_name} {contraints}'

        data = {
            'col_name': name, 'type_name': col.dtype_name,
            'contraints': cls._parse_constraints(col)}

        return cls.reformat(col_str.format(**data))

    @classmethod
    def _parse_constraints(cls, col) -> str:
        col_str = []

        contraint_parser = {
            'primary_key': cls._parse_primary_key,
            'not_null': cls._parse_not_null,
            'foreign_key': cls._parse_foreign_key
            }

        attrs = get_instance_attrs(col)

        for name, value in attrs.items():
            try:
                col_str.append(contraint_parser[name](col))
            except KeyError as e:
                continue
        return cls.reformat(' '.join(col_str).strip())

    @classmethod
    def _parse_primary_key(cls, col) -> str:
        """
        Parse attribute with primary key contraint
        """
        if col.primary_key:
            return 'primary key'
        return ''

    @classmethod
    def _parse_not_null(cls, col) -> str:
        if col.not_null:
            return 'not null'
        return ''

    @classmethod
    def _parse_foreign_key(cls, col) -> str:
        """        
        """
        if col.foreign_key is None:
            return ''

        command_str = 'references {parent_table}({parent_key})'
        if col.foreign_key is not None:
            data = {
                'parent_table': get_table_name(col.foreign_key[0]),
                'parent_key': col.foreign_key[1] if col.foreign_key[1] is not None else 'id'}
            return command_str.format(**data)
        return ''


class SQLDropTable(SQLParser):
    command = 'drop table if exists {}'

    @classmethod
    def parse(cls, table) -> str:
        table_name, attrs = super().parse(table)
        return cls.command.format(table_name)


class SQLGetInstances(SQLParser):
    """
    Get multiple rows from table
    """
    command = 'select * from {table_name}'
    command_filter = command + ' where {filter_str}'

    @classmethod
    def parse(cls, table, filter):
        data = {'table_name': get_table_name(table)}
        attrs = get_class_attrs(table)
        if filter is not None:
            # filter_str = []
            # for name, value in filter.items():
            #     if value is None:
            #         continue

            #     # if name == 'id':
            #     #     filter_str.append(f'{name} = {value}')
            #     # else:
            #     filter_str.append(f'{name} = {attrs[name].to_sqlite(value)}')

            # filter_str = ', '.join(filter_str)
            data['filter_str'] = cls.parse_filter(attrs, filter)
            command_str = cls.command_filter.format(**data)
        else:
            command_str = cls.command.format(**data)
        return cls.reformat(command_str)


class SQLGetInstance(SQLParser):
    """
    Get one row from table
    """
    command = 'select * from {table_name} where {filter_str}'

    @classmethod
    def parse(cls, table, filter: dict) -> str:
        data = {'table_name': get_table_name(table), 'filter_str': ''}
        attrs = get_class_attrs(table)
        filter_str = []
        # for name, value in filter.items():
        #     if value is None:
        #         continue

        #     # if name == 'id':
        #     #     filter_str.append(f'{name} = {value}')
        #     # else:
        #     filter_str.append(f'{name} = {attrs[name].to_sqlite(value)}')
        # data['filter_str'] = ' and '.join(filter_str)
        data['filter_str'] = cls.parse_filter(attrs, filter)
        return cls.reformat(cls.command.format(**data))


class SQLInsertInstance(SQLParser):
    command = 'insert into {} ({}) values ({})'

    @classmethod
    def parse(cls, object):
        table_name = get_table_name(object)
        attrs = get_instance_attrs(object)
        attrs_names = ', '.join(list(attrs.keys()))
        attrs_values = ', '.join([i.to_sqlite() for i in list(attrs.values())])
        return cls.command.format(table_name, attrs_names, attrs_values)


class SQLUpdateInstance(SQLParser):
    """
    Update row base on id
    """
    command = 'update {table_name} set {attrs_changes} where {table_name}.id = {id}'

    @classmethod
    def parse(cls, instance):
        data = {
            'table_name': get_table_name(instance),
            'attrs_str': '',
            'id': instance.id.to_sqlite()
            }

        attrs = get_instance_attrs(instance)

        values = []
        changes = []
        for name, attr in attrs.items():
            values.append(attr.to_sqlite())
            # print(f'{name} changed {attr._changed}')
            changes.append(attr._changed)

        attrs_changes = []
        for name, value, changed in zip(attrs.keys(), values, changes):
            if changed:
                attrs_changes.append(f'{name} = {value}')

        data['attrs_changes'] = ', '.join(attrs_changes)
        return cls.reformat(cls.command.format(**data))


class SQLDeleteInstances(SQLParser):
    """
    Delete row(s) based on filter
    """
    command = 'delete from {table_name}'
    command_filter = 'where {filter_str}'

    @classmethod
    def parse(cls, table, filter: Union[dict, None]):
        data = {'table_name': get_table_name(table)}
        attrs = get_class_attrs(table)

        if filter is not None:
            command_str = cls.command + ' ' + cls.command_filter
            data['filter_str'] = cls.parse_filter(attrs, filter)
        else:
            command_str = cls.command

        command_str.format(**data)
        return cls.reformat(command_str)
