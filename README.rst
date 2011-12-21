Redis Counters
==============

Counters for django models using redis.

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
