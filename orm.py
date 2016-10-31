'''
A Python object relational mapper for SQLite.

Author: Fernando Felix do Nascimento Junior
License: MIT License
Homepage: https://github.com/fernandojunior/python-sqlite-orm
'''
import sqlite3


def cut_attrs(obj, keys):
    return dict(i for i in vars(obj).items() if i[0] not in keys)


def render_schema(model):  # factory method to create table schemas for models
    schema = 'create table {table} (id integer primary key autoincrement, {columns});'  # noqa
    datatypes = {str: 'text', int: 'integer', float: 'real'}
    iscol = lambda key, value: key[0] is not '_' and value in datatypes.keys()
    colrender = lambda key, value: '%s %s' % (key, datatypes[value])
    cols = [colrender(*i) for i in vars(model).items() if iscol(*i)]
    values = {'table': model.__name__, 'columns': ', '.join(cols)}
    return schema.format(**values)


class Database(object):  # proxy class to access sqlite3.connect method

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.connected = False
        self.Model = type('Model%s' % str(self), (Model,), {'db': self})

    @property
    def connection(self):
        if self.connected:
            return self._connection
        self._connection = sqlite3.connect(*self.args, **self.kwargs)
        self._connection.row_factory = sqlite3.Row
        self.connected = True
        return self._connection

    def close(self):
        if self.connected:
            self.connection.close()
        self.connected = False

    def commit(self):
        self.connection.commit()

    def execute(self, sql, *args):
        return self.connection.execute(sql, args)

    def executescript(self, script):
        self.connection.cursor().executescript(script)
        self.commit()


class Manager(object):  # data mapper interface (generic repository) for models

    def __init__(self, db, model):
        self.db = db
        self.model = model
        self.tablename = model.__name__
        if not self._hastable():
            self.db.executescript(render_schema(self.model))

    def all(self):
        cursor = self.db.execute('select * from %s' % self.tablename)
        return (self.create(**row) for row in cursor.fetchall())

    def create(self, **kwargs):
        obj = object.__new__(self.model)
        obj.__dict__ = kwargs
        return obj

    def delete(self, obj):
        sql = 'DELETE from %s WHERE id = ?'
        self.db.execute(sql % self.tablename, obj.id)

    def get(self, id):
        sql = 'select * from %s where id = ?' % self.tablename
        cursor = self.db.execute(sql, id)
        row = cursor.fetchone()
        if not row:
            msg = 'Object%s with id does not exist: %s' % (self.model, id)
            raise ValueError(msg)
        return self.create(**row)

    def has(self, id):
        sql = 'select id from %s where id = ?' % self.tablename
        cursor = self.db.execute(sql, id)
        return True if cursor.fetchall() else False

    def save(self, obj):
        if hasattr(obj, 'id') and self.has(obj.id):
            msg = 'Object%s id already registred: %s' % (self.model, obj.id)
            raise ValueError(msg)
        copy_ = cut_attrs(obj, 'id')
        keys = '(%s)' % ', '.join(copy_.keys())  # (key1, ...)
        refs = '(%s)' % ', '.join('?' for i in range(len(copy_)))  # (?, ...)
        sql = 'insert into %s %s values %s' % (self.tablename, keys, refs)
        cursor = self.db.execute(sql, *copy_.values())
        obj.id = cursor.lastrowid
        return obj

    def update(self, obj):
        copy_ = cut_attrs(obj, 'id')
        keys = '= ?, '.join(copy_.keys()) + '= ?'  # key1 = ?, ...
        sql = 'UPDATE %s SET %s WHERE id = ?' % (self.tablename, keys)
        self.db.execute(sql, *(list(copy_.values()) + [obj.id]))

    def _hastable(self):
        sql = 'select name len FROM sqlite_master where type = ? AND name = ?'
        cursor = self.db.execute(sql, 'table', self.tablename)
        return True if cursor.fetchall() else False


class QueryBuilder(object):

    EQUAL = "="
    NOT_EQUAL = "!="
    AND = "AND"
    OR = "OR"
    ALL = "*"
    LIMIT = "limit"

    def __init__(self, model):
        self.model = model
        self.query_tokens = []

    def filter(self, comparator=EQUAL, boolean=AND, **kwargs):
        valid_keyvals = dict(i for i in vars(self.model).items() if i[0][0] is not '_')

        for key, value in kwargs.items():
            if key in valid_keyvals.keys():
                self.query_tokens += [key] + [comparator] + [str(value)] + [boolean]
            else:
                raise(ValueError("Invalid column \"%s\"" % key))

        if(len(self.query_tokens) > 0):
            self.query_tokens = self.query_tokens[:-1]

        return self

    def limit(self, num):
        self.query_tokens += [self.LIMIT] + [str(num)]

        return self

    def select(self, *args):
        valid_keyvals = dict(i for i in vars(self.model).items() if i[0][0] is not '_')

        columns = []
        if(len(args)):
            for value in args:
                if value in valid_keyvals.keys():
                    columns += [value]
                else:
                    raise(ValueError("Invalid column \"%s\"" % key))
        else:
            columns = self.ALL

        sql = "select %s from %s where %s;"
        cursor = self.model.db.execute(sql % (" ,".join(columns) if len(columns) > 1 else columns[0], self.model.__name__, " ".join(self.query_tokens))
        return self.model.manager().create(**row) for row in cursor.fetchall())

    def update(self, **kwargs):
        pass

    def delete(self):
        sql = "delete from %s where %s"
        cursor = self.model.db.execute(sql % (self.model.__name__, " ".join(self.query_tokens)))


class Model(object):  # abstract entity model with an active record interface

    db = None

    def delete(self):
        return self.__class__.manager().delete(self)

    def save(self):
        return self.__class__.manager().save(self)

    def update(self):
        return self.__class__.manager().update(self)

    @property
    def public(self):
        return dict(i for i in vars(self).items() if i[0][0] is not '_')

    def __repr__(self):
        return str(self.public)

    @classmethod
    def manager(cls, db=None):
        return Manager(db if db else cls.db, cls)
