Redis Counters
==============

Counters for django models using redis.


Setup
-----

settings.py::

    REDIS = {
        'host': 'localhost',
        'port': 6379,
        'db': 1,
        'socket_timeout': 2,
    }

    COUNTERS_REDISES = {
        'default': dict(REDIS, db=4),
        'krasnoyarsk': dict(REDIS, host='krasnoyarsk.yoursite.ru', db=4, socket_timeout=5),
        'moscow': dict(REDIS, host='moscow.yoursite.ru', db=4, socket_timeout=5)
    }
    COUNTERS_REDISES.pop(SERVER_LOCATION)


Usage
-----

models.py::

    import counters

    @counters.add('hits')
    class Item(models.Model):
        ...

views.py::

    def list(request):
        items = Item.objects.filter(...)
        counters.fill(items)
        ...

    def detail(request, pk):
        item = Item.objects.get(pk=pk)
        item.incr_hits()

list.html::

    {% for item in items %}
        {{ item.title }} {{ item.hits }}<br>
    {% endfor $}

detail.html::

    <h1>{{ item.title }}</h1>

    Hits: {{ item.hits }}
