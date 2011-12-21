# -*- coding: utf-8 -*-
from redis import RedisError

from django.core.management.base import BaseCommand, CommandError
from django.db.models.loading import cache

from counters import redis_conn, redis_conns


def check_results(results):
    error = next((r for r in results if isinstance(r, RedisError)), None)
    if error:
        raise error
    return results


class Command(BaseCommand):
    help = 'Replicates counters to other redises'

    def handle(self,  **options):
        verbosity = int(options.get('verbosity', 1))

        dest_conns = [(name, conn) for name, conn in redis_conns.items() if name not in ('default', 'old')]

        ### Подготовим множества к репликации на разные сервера (пендинги)
        incr_keys = redis_conn.keys('*.incr')
        for incr_key in incr_keys:
            if verbosity >= 1:
                print "> Creating pendings for %s..." % incr_key
            txn = redis_conn.pipeline()
            for name, conn in dest_conns:
                pending_key = '%s.%s' % (incr_key, name)
                # Добавляем к старому пендингу (если вдруг остался), новые инкременты
                txn.zunionstore(pending_key, [incr_key, pending_key])
            # Сносим накопитель, теперь всё в пендингах
            txn.delete(incr_key)
            check_results(txn.execute())

        ### Выполняем пендинги для каждого внешнего сервера
        for name, conn in dest_conns:
            pending_keys = redis_conn.keys('*.incr.%s' % name)

            for pending_key in pending_keys:
                incr_key = pending_key[:-1-len(name)] # -1 - для точки
                key = incr_key[:-len('.incr')]

                if verbosity >= 1:
                    print "> Replicating pendings %s..." % pending_key
                if verbosity >= 2:
                    print "Fetch..."
                incrs = redis_conn.zrange(pending_key, 0, -1, withscores=True)

                local_pipe = redis_conn.pipeline(transaction=False)

                if verbosity >= 2:
                    print "Replicate..."
                dest_txn = conn.pipeline()
                for member, incr in incrs:
                    if int(incr) > 0:
                        dest_txn.zincrby(key, member, incr)
                    else:
                        dest_txn.zrem(key, member)
                        dest_txn.zrem(incr_key, member)
                        # Нужно также удалить локально ещё раз,
                        # на тот случай если мы получили репликацию пока висела метка на удаление
                        local_pipe.zrem(key, member)
                        local_pipe.zrem(incr_key, member)
                check_results(dest_txn.execute())
                check_results(local_pipe.execute())

                # Сносим пендинг
                if verbosity >= 2:
                    print "Drop pending..."
                redis_conn.delete(pending_key)
