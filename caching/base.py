import functools
import logging

from django.conf import settings
from django.core.cache import cache
from django.db import models
from django.db.models import signals
from django.db.models.sql import query
from django.utils import encoding

from .invalidation import invalidator, flush_key, make_key, byid


class NullHandler(logging.Handler):

    def emit(self, record):
        pass


log = logging.getLogger('caching')
log.addHandler(NullHandler())

FOREVER = 0
NO_CACHE = -1
CACHE_PREFIX = getattr(settings, 'CACHE_PREFIX', '')
FETCH_BY_ID = getattr(settings, 'FETCH_BY_ID', False)
CACHE_EMPTY_QUERYSETS = getattr(settings, 'CACHE_EMPTY_QUERYSETS', False)


class CachingManager(models.Manager):

    # Tell Django to use this manager when resolving foreign keys.
    use_for_related_fields = True

    def get_query_set(self):
        return CachingQuerySet(self.model)

    def contribute_to_class(self, cls, name):
        signals.post_save.connect(self.post_save, sender=cls)
        signals.post_delete.connect(self.post_delete, sender=cls)
        return super(CachingManager, self).contribute_to_class(cls, name)

    def post_save(self, instance, **kwargs):
        '''
        That is usual CM invalidation extended
        to also invalidate user queries on INSERT
        '''

        self.invalidate(instance)
        # insert
        if kwargs.get('created'):
            user_keys = instance._cache_keys_of_user()
            if user_keys:
                invalidator.invalidate_keys(user_keys)

    def post_delete(self, instance, **kwargs):
        self.invalidate(instance)

    def invalidate(self, *objects):
        """Invalidate all the flush lists associated with ``objects``."""
        keys = [k for o in objects for k in o._cache_keys()]
        invalidator.invalidate_keys(keys)

    def raw(self, raw_query, params=None, *args, **kwargs):
        return CachingRawQuerySet(raw_query, self.model, params=params,
                                  using=self._db, *args, **kwargs)

    def cache(self, timeout=None):
        return self.get_query_set().cache(timeout)

    def no_cache(self):
        return self.cache(NO_CACHE)


class CacheMachine(object):
    """
    Handles all the cache management for a QuerySet.

    Takes the string representation of a query and a function that can be
    called to get an iterator over some database results.
    """

    def __init__(self, query_string, iter_function, timeout=None, db='default', by_pk=False):
        self.by_pk = by_pk
        self.query_string = query_string
        self.iter_function = iter_function
        self.timeout = timeout
        self.db = db

    def query_key(self):
        """
        Generate the cache key for this query.

        Database router info is included to avoid the scenario where related
        cached objects from one DB (e.g. slave) are saved in another DB (e.g.
        master), throwing a Django ValueError in the process. Django prevents
        cross DB model saving among related objects.
        """
        query_db_string = u'qs:%s::db:%s' % (self.query_string, self.db)
        return make_key(query_db_string, with_locale=False)

    def __iter__(self):
        try:
            query_key = self.query_key()
        except query.EmptyResultSet:
            raise StopIteration

        # Try to fetch from the cache.
        cached = cache.get(query_key)
        if cached is not None:
            log.debug('cache hit: %s' % self.query_string)
            for obj in cached:
                obj.from_cache = True
                yield obj
            return

        # Do the database query, cache it once we have all the objects.
        iterator = self.iter_function()

        to_cache = []
        try:
            while True:
                obj = iterator.next()
                obj.from_cache = False
                to_cache.append(obj)
                yield obj
        except StopIteration:
            if to_cache or CACHE_EMPTY_QUERYSETS:
                self.cache_objects(to_cache)
            raise

    def cache_objects(self, objects):
        """Cache query_key => objects, then update the flush lists.
        It calculate depenedency for the query (get by primary key has less dependency)
        and add the dependency to the invalidator.

        Cache query_key => objects, then update the flush lists.
        added invalidate user keys
        """

        if len(objects) == 1 and self.by_pk: # if pk & more then one result - it must be error!!
            # ignore all FKs - use only pk in cache key.
            depend = [dummyObj4Key(obj.cache_key) for obj in objects if obj]
        else:
            keys = set([k for o in objects for k in o._cache_keys_of_user()])
            depend = [dummyObj4Key(k) for k in keys if k]+objects

        query_key = self.query_key()
        query_flush = flush_key(self.query_string)
        invalidator.cache_objects(depend, query_key, query_flush)

        should_cache = depend or CACHE_EMPTY_QUERYSETS and not self.by_pk
        # if no dependency don't cache since there would be nothing to invalidate it.
        if should_cache:
            cache.add(query_key, objects, timeout=self.timeout)

pks_lst = set(['pk', 'id', 'id__exact'])

class CachingQuerySet(models.query.QuerySet):

    def __init__(self, *args, **kw):
        super(CachingQuerySet, self).__init__(*args, **kw)
        self.timeout = None
        self._by_pk = False

    def filter_or_exclude(self, negate, *args, **kwargs):
        by_pk=True
        if negate:
            by_pk=False
        for key in kwargs.keys():
            if key not in pks_lst:
                by_pk=False
        self._by_pk=by_pk

        res = super(CachingQuerySet, self)._filter_or_exclude(negate, *args, **kwargs)
        return res

    def flush_key(self):
        return flush_key(self.query_key())
    def query_key(self):
        clone = self.query.clone()
        sql, params = clone.get_compiler(using=self.db).as_sql()
        return sql % params

    def iterator(self):
        iterator = super(CachingQuerySet, self).iterator
        if self.timeout == NO_CACHE:
            return iter(iterator())
        else:
            try:
                # Work-around for Django #12717.
                query_string = self.query_key()
            except query.EmptyResultSet:
                return iterator()
            if FETCH_BY_ID:
                iterator = self.fetch_by_id
            return iter(CacheMachine(query_string, iterator, self.timeout, db=self.db, by_pk=self._by_pk))

    def fetch_by_id(self):
        """
        Run two queries to get objects: one for the ids, one for id__in=ids.

        After getting ids from the first query we can try cache.get_many to
        reuse objects we've already seen.  Then we fetch the remaining items
        from the db, and put those in the cache.  This prevents cache
        duplication.
        """
        # Include columns from extra since they could be used in the query's
        # order_by.
        vals = self.values_list('pk', *self.query.extra.keys())
        pks = [val[0] for val in vals]
        keys = dict((byid(self.model._cache_key(pk)), pk) for pk in pks)
        cached = dict((k, v) for k, v in cache.get_many(keys).items()
                      if v is not None)

        # Pick up the objects we missed.
        missed = [pk for key, pk in keys.items() if key not in cached]
        if missed:
            others = self.fetch_missed(missed)
            # Put the fetched objects back in cache.
            new = dict((byid(o), o) for o in others)
            cache.set_many(new)
        else:
            new = {}

        # Use pks to return the objects in the correct order.
        objects = dict((o.pk, o) for o in cached.values() + new.values())
        for pk in pks:
            yield objects[pk]

    def fetch_missed(self, pks):
        # Reuse the queryset but get a clean query.
        others = self.all()
        others.query.clear_limits()
        # Clear out the default ordering since we order based on the query.
        others = others.order_by().filter(pk__in=pks)
        if hasattr(others, 'no_cache'):
            others = others.no_cache()
        if self.query.select_related:
            others.dup_select_related(self)
        return others

    def count(self):
        timeout = getattr(settings, 'CACHE_COUNT_TIMEOUT', None)
        super_count = super(CachingQuerySet, self).count
        query_string = 'count:%s' % self.query_key()
        if self.timeout == NO_CACHE or timeout is None:
            return super_count()
        else:
            return cached_with(self, super_count, query_string, timeout)

    def cache(self, timeout=None):
        qs = self._clone()
        qs.timeout = timeout
        return qs

    def no_cache(self):
        return self.cache(NO_CACHE)

    def _clone(self, *args, **kw):
        qs = super(CachingQuerySet, self)._clone(*args, **kw)
        qs.timeout = self.timeout
        qs._by_pk = self._by_pk

        return qs

def _cache_key(cls, pk):
  """
  Return a string that uniquely identifies the object.

  For the Addon class, with a pk of 2, we get "o:addons.addon:2".
  """
  key_parts = ('o', cls._meta, pk)
  return ':'.join(map(encoding.smart_unicode, key_parts))

from django.contrib.auth.models import User
# could be change extended to other FKS which are not CM
_externalFKsClasses=[User]

class CachingMixin:
    """Inherit from this class to get caching and invalidation helpers."""

    def flush_key(self):
        return flush_key(self)

    @property
    def cache_key(self):
        """Return a cache key based on the object's primary key."""
        return self._cache_key(self.pk)

    @classmethod
    def _cache_key(cls, pk):
        """
        Return a string that uniquely identifies the object.

        For the Addon class, with a pk of 2, we get "o:addons.addon:2".
        """
        return _cache_key(cls,pk)

    def _cache_keys(self):
        """Return the cache key for self plus all related foreign keys."""
        fks = dict((f, getattr(self, f.attname)) for f in self._meta.fields
                    if isinstance(f, models.ForeignKey))

        keys = [fk.rel.to._cache_key(val) for fk, val in fks.items()
                if val is not None and hasattr(fk.rel.to, '_cache_key')]
        return (self.cache_key,) + tuple(keys)

    def _cache_keys_of_user(self):
        """Method for tracking FK to the user class which is not cached by CM,
        indicating that we might want this user queries invalidated on insert,
        Return all the user keys that should invalidate a query.
        """
        fks = dict((f, getattr(self, f.attname)) for f in self._meta.fields
                    if isinstance(f, models.ForeignKey))
        model_name = self.__class__.__name__
        user_keys = ['CM_%s:%s' % (model_name, _cache_key(fk.rel.to, val)) for fk, val in fks.items()
                   if val is not None and fk.rel.to in _externalFKsClasses]
        if not user_keys:
            return []
        return set(user_keys)

    
class dummyObj4Key(object):
    '''
    dummy "adapter" transfer object so current CM code can invalidate the keys
    wrap keys for direct invalidation by key without actual object.
    '''

    def __init__(self, key):
        self.cache_key = self.pk = key

    def _cache_keys(self):
        return self.cache_key,

    def _cache_key(self, pk):
        return pk

    def flush_key(self):
        return flush_key(self)

class CachingRawQuerySet(models.query.RawQuerySet):

    def __iter__(self):
        iterator = super(CachingRawQuerySet, self).__iter__
        sql = self.raw_query % tuple(self.params)
        for obj in CacheMachine(sql, iterator):
            yield obj
        raise StopIteration


def _function_cache_key(key):
    return make_key('f:%s' % key, with_locale=True)


def cached(function, key_, duration=None):
    """Only calls the function if ``key`` is not already in the cache."""
    key = _function_cache_key(key_)
    val = cache.get(key)
    if val is None:
        log.debug('cache miss for %s' % key)
        val = function()
        cache.set(key, val, duration)
    else:
        log.debug('cache hit for %s' % key)
    return val


def cached_with(obj, f, f_key, timeout=None):
    """Helper for caching a function call within an object's flush list."""
    try:
        obj_key = (obj.query_key() if hasattr(obj, 'query_key')
                   else obj.cache_key)
    except AttributeError:
        log.warning(u'%r cannot be cached.' % encoding.smart_str(obj))
        return f()

    key = '%s:%s' % tuple(map(encoding.smart_str, (f_key, obj_key)))
    # Put the key generated in cached() into this object's flush list.
    invalidator.add_to_flush_list(
        {obj.flush_key(): [_function_cache_key(key)]})
    return cached(f, key, timeout)


class cached_method(object):
    """
    Decorator to cache a method call in this object's flush list.

    The external cache will only be used once per (instance, args).  After that
    a local cache on the object will be used.

    Lifted from werkzeug.
    """
    def __init__(self, func):
        self.func = func
        functools.update_wrapper(self, func)

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        _missing = object()
        value = obj.__dict__.get(self.__name__, _missing)
        if value is _missing:
            w = MethodWrapper(obj, self.func)
            obj.__dict__[self.__name__] = w
            return w
        return value


class MethodWrapper(object):
    """
    Wraps around an object's method for two-level caching.

    The first call for a set of (args, kwargs) will use an external cache.
    After that, an object-local dict cache will be used.
    """
    def __init__(self, obj, func):
        self.obj = obj
        self.func = func
        functools.update_wrapper(self, func)
        self.cache = {}

    def __call__(self, *args, **kwargs):
        k = lambda o: o.cache_key if hasattr(o, 'cache_key') else o
        arg_keys = map(k, args)
        kwarg_keys = [(key, k(val)) for key, val in kwargs.items()]
        key_parts = ('m', self.obj.cache_key, self.func.__name__,
                     arg_keys, kwarg_keys)
        key = ':'.join(map(encoding.smart_unicode, key_parts))
        if key not in self.cache:
            f = functools.partial(self.func, self.obj, *args, **kwargs)
            self.cache[key] = cached_with(self.obj, f, key)
        return self.cache[key]
