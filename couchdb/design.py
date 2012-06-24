# -*- coding: utf-8 -*-
#
# Copyright (C) 2008-2009 Christopher Lenz
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.

"""Utility code for managing design documents."""

from copy import deepcopy
from inspect import getsource
from itertools import groupby
from operator import attrgetter
from textwrap import dedent
from types import FunctionType

__all__ = ['ViewDefinition']
__docformat__ = 'restructuredtext en'


class DesignDefinition(object):
    def __init__(self, design, name, map_fun=None, reduce_fun=None,
                 language='javascript', options=None,
                 filter_fun=None, **defaults):
        """Initialize the design definition.

        Note that the code in `map_fun` and `reduce_fun` is automatically
        dedented, that is, any common leading whitespace is removed from each
        line.

        :param design: the name of the design document
        :param name: the name of the view
        :param map_fun: the map function code (optional if filter_fun)
        :param reduce_fun: the reduce function code (optional)
        :param filter_fun: the filter function code (optional if map_fun)
        :param language: the name of the language used
        :param wrapper: an optional callable that should be used to wrap the
                        result rows
        :param options: view specific options (e.g. {'collation':'raw'})
        """
        if design.startswith('_design/'):
            design = design[8:]
        self.design = design
        self.name = name

        # Must have a map or filter
        assert not (map_fun is None and filter_fun is None)

        # set map function
        if isinstance(map_fun, FunctionType):
            map_fun = _strip_decorators(getsource(map_fun).rstrip())
        if map_fun:
            map_fun = dedent(map_fun.lstrip('\n'))
        self.map_fun = map_fun

        # set reduce function
        if isinstance(reduce_fun, FunctionType):
            reduce_fun = _strip_decorators(getsource(reduce_fun).rstrip())
        if reduce_fun:
            reduce_fun = dedent(reduce_fun.lstrip('\n'))
        self.reduce_fun = reduce_fun

        # set filter function
        if isinstance(filter_fun, FunctionType):
            filter_fun = _strip_decorators(getsource(filter_fun).rstrip())
        if filter_fun:
            filter_fun = dedent(filter_fun.lstrip('\n'))
        self.filter_fun = filter_fun

        self.language = language
        self.options = options
        self.defaults = defaults

    def __repr__(self):
        return '<%s %r>' % (type(self).__name__, '/'.join([
            '_design', self.design, '_view', self.name
        ]))

    def get_doc(self, db):
        """Retrieve and return the design document corresponding to this view
        definition from the given database.

        :param db: the `Database` instance
        :return: a `client.Document` instance, or `None` if the design document
                 does not exist in the database
        :rtype: `Document`
        """
        return db.get('_design/%s' % self.design)

    def sync(self, db):
        """Ensure that the view stored in the database matches the view defined
        by this instance.

        :param db: the `Database` instance
        """
        type(self).sync_many(db, [self])

    # TODO: change name view for design_definition
    @staticmethod
    def sync_many(db, views, remove_missing=False, callback=None):
        """Ensure that the views stored in the database that correspond to a
        given list of `ViewDefinition` instances match the code defined in
        those instances.

        This function might update more than one design document. This is done
        using the CouchDB bulk update feature to ensure atomicity of the
        operation.

        :param db: the `Database` instance
        :param views: a sequence of `ViewDefinition` instances
        :param remove_missing: whether views found in a design document that
                               are not found in the list of `ViewDefinition`
                               instances should be removed
        :param callback: a callback function that is invoked when a design
                         document gets updated; the callback gets passed the
                         design document as only parameter, before that doc
                         has actually been saved back to the database
        """
        docs = []

        for design, views in groupby(views, key=attrgetter('design')):
            doc_id = '_design/%s' % design
            doc = db.get(doc_id, {'_id': doc_id})
            orig_doc = deepcopy(doc)
            languages = set()

            missing_views = list(doc.get('views', {}).keys())
            missing_filters = list(doc.get('filters', {}).keys())
            for view in views:
                funcs = {}
                if view.map_fun:
                    funcs['map'] = view.map_fun
                if view.reduce_fun:
                    funcs['reduce'] = view.reduce_fun

                if view.options:
                    funcs['options'] = view.options

                if view.filter_fun:
                    #funcs[view.name] = view.filter_fun
                    if view.name in missing_filters:
                        missing_filters.remove(view.name)
                    doc.setdefault('filters', {})[view.name] = view.filter_fun
                else:
                    if view.name in missing_views:
                        missing_views.remove(view.name)
                    doc.setdefault('views', {})[view.name] = funcs
                languages.add(view.language)

            if remove_missing:
                for name in missing_views:
                    del doc['views'][name]
                for name in missing_filters:
                    del doc['filters'][name]

            elif (missing_views or missing_filters) and 'language' in doc:
                languages.add(doc['language'])

            if len(languages) > 1:
                raise ValueError('Found different language views in one '
                                 'design document (%r)', list(languages))
            doc['language'] = list(languages)[0]

            if doc != orig_doc:
                if callback is not None:
                    callback(doc)
                docs.append(doc)

        db.update(docs)


class ViewDefinition(DesignDefinition):
    r"""Definition of a view stored in a specific design document.

    An instance of this class can be used to access the results of the view,
    as well as to keep the view definition in the design document up to date
    with the definition in the application code.

    >>> from couchdb import Server
    >>> server = Server()
    >>> db = server.create('python-tests')

    >>> view = ViewDefinition('tests', 'all', '''function(doc) {
    ...     emit(doc._id, null);
    ... }''')
    >>> view.get_doc(db)

    The view is not yet stored in the database, in fact, design doc doesn't
    even exist yet. That can be fixed using the `sync` method:

    >>> view.sync(db)

    >>> design_doc = view.get_doc(db)
    >>> design_doc                                          #doctest: +ELLIPSIS
    <Document '_design/tests'@'...' {...}>
    >>> print design_doc['views']['all']['map']
    function(doc) {
        emit(doc._id, null);
    }

    If you use a Python view server, you can also use Python functions instead
    of code embedded in strings:

    >>> def my_map(doc):
    ...     yield doc['somekey'], doc['somevalue']
    >>> view = ViewDefinition('test2', 'somename', my_map, language='python')
    >>> view.sync(db)
    >>> design_doc = view.get_doc(db)
    >>> design_doc                                          #doctest: +ELLIPSIS
    <Document '_design/test2'@'...' {...}>
    >>> print design_doc['views']['somename']['map']
    def my_map(doc):
        yield doc['somekey'], doc['somevalue']

    Use the static `sync_many()` method to create or update a collection of
    views in the database in an atomic and efficient manner, even across
    different design documents.

    >>> del server['python-tests']
    """

    def __init__(self, design, name, map_fun, reduce_fun=None,
                 language='javascript', wrapper=None, options=None,
                 **defaults):
        DesignDefinition.__init__(self, design, name, map_fun,
                reduce_fun, language, options, **defaults)
        self.wrapper = wrapper

    def __call__(self, db, **options):
        """Execute the view in the given database.

        :param db: the `Database` instance
        :param options: optional query string parameters
        :return: the view results
        :rtype: `ViewResults`
        """
        merged_options = self.defaults.copy()
        merged_options.update(options)
        # replace default wrapper if the view is called with a new wrapper
        if "wrapper" not in  merged_options:
            merged_options["wrapper"] = self.wrapper
        return db.view('/'.join([self.design, self.name]), **merged_options)

class FilterDefinition(DesignDefinition):
    def __init__(self, design, name, filter_fun, language='javascript',
            options=None, **defaults):
        DesignDefinition.__init__(self, design, name, filter_fun=filter_fun,
                             language=language, options=options, **defaults)

def _strip_decorators(code):
    retval = []
    beginning = True
    for line in code.splitlines():
        if beginning and not line.isspace():
            if line.lstrip().startswith('@'):
                continue
            beginning = False
        retval.append(line)
    return '\n'.join(retval)
