
def get_table_name(table) -> str:
    """
    Return table name and attributes of class/instance
    """
    if table.__class__.__name__ == 'type':
        # Class
        table_name = table.__meta__.db_name if table.__meta__.db_name is not None else table.__name__
    else:
        # Instance
        table_name = table.__meta__.db_name if table.__meta__.db_name is not None else table.__class__.__name__

    return table_name


def get_instance_attrs(object) -> dict:
    """
    Return a model's instance attrs, not include attrs start with '_'
    """
    attrs = object.__dict__
    output = {}
    for k, v in attrs.items():
        if k[0] != '_':
            output[k] = v
    return output


def filter_attrs(attrs) -> dict:
    attr_names = list(attrs.keys())
    attr_names = list(filter(lambda x: x[:2] != '__', attr_names))
    attr_names = list(filter(lambda x: attrs[x].__class__.__name__ not in ['classmethod', 'function'], attr_names))

    output = {}
    for name in attr_names:
        output[name] = attrs[name]
    return output


def _get_class_attrs(object):
    pass


def get_class_attrs(object):
    """
    Return a dict of user-defined class attributes including parent's class attribute
    """
    if object.__class__.__name__ == 'type':
        # For class object
        attrs = object.__dict__
        parents = object.__bases__
    else:
        # For instance object
        attrs = object.__class__.__dict__
        parents = object.__class__.__bases__

    # attr_names = list(attrs.keys())
    # attr_names = list(filter(lambda x: x[:2] != '__', attr_names))
    # attr_names = list(filter(lambda x: attrs[x].__class__.__name__ not in ['classmethod', 'function'], attr_names))
    # output = {}
    # for name in attr_names:
    #     output[name] = attrs[name]

    output = {}
    # Parent class object
    for parent in parents:
        parent_attrs = filter_attrs(parent.__dict__)
        output.update(parent_attrs)

    # Current class/instance object, overwrite parents' attributes
    output.update(filter_attrs(attrs))

    return output
