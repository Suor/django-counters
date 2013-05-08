# -*- coding: utf-8 -*-
"""
Модуль счётчиков

Счётчики хранятся в упорядоченых множествах в редисе.
"""
import redis

from django.utils.functional import curry
from django.db.models.signals import post_delete
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


# Создаём редис-клиенты
try:
    redis_confs = settings.COUNTERS_REDISES
except AttributeError:
    raise ImproperlyConfigured('You must specify COUNTERS_REDISES setting to use counters')

redis_conns = dict((name, redis.Redis(**conf)) for name, conf in redis_confs.items())
redis_conn = redis_conns['default']


def add(field_name):
    """
    Декоратор, добавляет свойство счётчика и соответсвующую функцию инкремента к классу.
    """
    def decorator(cls):
        # Навешиваем методы и свойства
        getter = curry(_method, _get, field_name)
        setter = curry(_set_method, field_name)
        incrementer = curry(_method, _incr, field_name)

        setattr(cls, 'get_' + field_name, getter)
        setattr(cls, field_name, property(getter, setter))
        setattr(cls, 'incr_' + field_name, incrementer)

        # Навешиваем методы класса
        key_getter = curry(_key, field_name)
        cls_getter = curry(_class_method, _get, field_name)
        cls_incrementer = curry(_class_method, _incr, field_name)

        setattr(cls, 'key_for_' + field_name, classmethod(key_getter))
        setattr(cls, 'get_%s_for_pk' % field_name, classmethod(cls_getter))
        setattr(cls, 'incr_%s_for_pk' % field_name, classmethod(cls_incrementer))

        if not hasattr(cls, '_counters'):
            cls._counters = []
        cls._counters.append(field_name)

        post_delete.connect(curry(_post_delete, field_name), sender=cls,
                            weak=False, dispatch_uid=(cls, field_name))

        return cls
    return decorator


def fill(objects, field_name, domain=None):
    """
    Заполняем кеш счётчика во всех объектах в итерируемом.
    """
    pipe = redis_conn.pipeline(transaction=False)
    for obj in objects:
        pipe.zscore(_key(field_name, obj, domain=domain), obj.pk)
    scores = pipe.execute()

    # Прописываем значения в кеш свойств
    cache_name = '_' + field_name
    for i, obj in enumerate(objects):
        setattr(obj, cache_name, int(scores[i] or 0))


def _key(field_name, obj_or_cls, domain=None):
    opts = obj_or_cls._meta
    if domain:
        return '%s.%s.%s:%s' % (opts.app_label, opts.module_name, domain, field_name)
    else:
        return '%s.%s:%s' % (opts.app_label, opts.module_name, field_name)


def _get(key, member):
    return int(redis_conn.zscore(key, member) or 0)

def _incr(key, member):
    pipe = redis_conn.pipeline(transaction=False)
    pipe.zincrby(key, member)
    pipe.zincrby(key + '.incr', member)
    return int(pipe.execute()[0])

def _remove(key, member):
    txn = redis_conn.pipeline()
    txn.zrem(key, member)
    txn.zadd(key + '.incr', member, -1000000) # планируем удаление на других серверах
    txn.execute()


def _method(getter, field_name, self, domain=None):
    """
    Выдаёт значение счётчик и возможно инкрементирует его (в зависимости от переданного геттера).
    Вообще предназначена, чтобы каррировать её и сделать свойство или метод объекта.
    """
    cache_name = '_' + field_name
    if not hasattr(self, cache_name) or getter == _incr:
        key = _key(field_name, self, domain=domain)
        setattr(self, cache_name, getter(key, self.pk))
    return getattr(self, cache_name)

def _set_method(field_name, self, value):
    setattr(self, '_' + field_name, value)

def _class_method(getter, field_name, cls, pk, domain=None):
    """
    То же для класса, для использования когда объекта нет, только его pk.
    """
    key = _key(field_name, cls, domain=domain)
    return getter(key, pk)

def _post_delete(field_name, sender, instance, **kwargs):
    key = _key(field_name, instance)
    _remove(key, instance.pk)
