#!/usr/bin/python

import rados
from xattr import pyxattr_compat as xattr
import os
import logging
from os import path
import threading
import errno


logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('%s.log' % __file__)
fh.setLevel(logging.INFO)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)


def mkdir(dir_name):
    """Makes dir and check for various errors, that simple!"""
    if not os.path.exists(dir_name):
        try:
            os.makedirs(dir_name)
        except (IOError, OSError) as e:
            if e.errno == errno.EEXIST and os.path.isdir(dir_name):
                pass
            if e.errno == errno.EACCES:
                logger.error(
                    "Insufficent permissions! Cannot create directory %s", dir_name)
                return False
            if e.errno == errno.ENOTDIR:
                logger.error("Not a directory, \"%s\"", e.filename)
                return False
            logger.exception("Unhandled exception in %(funcName)s")
            return False
    return True


def set_xattr(filename, name, value):
    """soft wrapper for underlying xattr module with user namespace"""
    try:
        return xattr.set(filename, name, value, flags=xattr.XATTR_CREATE, namespace=xattr.NS_USER)
    except IOError:
        logger.exception("Error while setting Xattr")


def write_me(obj, store_dir):
    """Writes objects contents using aio_read to store_dir, with parsing object name and extract xattrs"""
    my_ioctx = cluster.open_ioctx(poolname)

    def parse_obj_to_fn(obj_name):
        """Parse object name and try to extract bucket path. Ignores shadow files completely"""
        if '__shadow_' in obj_name:
            return False
        try:
            some1, some2, bucket_path = obj_name.split('.', 2)
            # bucketname, filepath = bucket_path.split('/', 1)
            return path.join(store_dir, *bucket_path.split('/'))
        except (ValueError, AttributeError):
            logger.exception(
                "Error while parsing name for object %s!", obj_name)
            return False

    def aio_read_cb(completion, data_read):
        """Callback copycat'd from ceph tests
        http://workbench.dachary.org/ceph/ceph/blob/master/src/test/pybind/test_rados.py#L716"""
        with lock:
            completion.wait_for_safe()
            completion.wait_for_complete()
            with open(filename, 'wb') as f:
                f.write(data_read)
                f.flush()
            for attrname, attrvalue in obj.get_xattrs():
                set_xattr(filename, attrname, attrvalue)
            lock.notify()
            return True

    lock = threading.Condition()
    filename = parse_obj_to_fn(obj.key)
    if filename:
        size, tm = obj.stat()
        # if already downloaded - skip
        if path.isfile(filename) and os.stat(filename).st_size == size:
            return True
        mkdir(path.dirname(filename))

        comp = my_ioctx.aio_read(obj.key, size, 0, aio_read_cb)
        comp.wait_for_complete_and_cb()

    my_ioctx.close()
    return True


def consumer(n):
    """Worker thread, takes number for identify himself. Basically plain adaptation of tutorial
    http://sdiehl.github.io/gevent-tutorial/#queues"""
    try:
        while True:
            obj = tasks.get(timeout=1)
            write_me(obj, STORAGE_DIR)
            gevent.sleep(0)
    except Empty:
        pass


def producer(iterator):
    """Simple producer, can't afford more due to splitting iterator problems"""
    logger.info('start putting tasks in Queue')
    i = 0
    for obj in iterator:
        if i % 1000 == 0:
            logger.info('Processed another %d items', i)
        tasks.put(obj)
        i += 1
    logger.info('Processed total %d items!', i)


if __name__ == '__main__':
    THREAD_NUM = 20
    POOL_NAME = '.rgw.buckets'
    cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()

    poolname = POOL_NAME
    STORAGE_DIR = '/tmp'  # can be overrided via command line args
    # you can skip already processed objects(and checks) if adjust this number
    # in my understanding, object_iterator ALWAYS ordered
    START_ITEM = 0

    ioctx = cluster.open_ioctx(poolname)

    object_iterator = ioctx.list_objects()

    import argparse
    import itertools

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start", help="start number of main iterator")
    parser.add_argument("-t", "--threads", help="number of threads")
    parser.add_argument("-e", "--end", help="end number of main iterator")
    parser.add_argument("-d", "--storage", help="storage directory")
    parser.add_argument("-p", "--pool", help="pool to dump")
    args = parser.parse_args()
    if args.pool:
        poolname = args.pool
    if args.threads:
        THREAD_NUM = int(args.threads)
    if args.start:
        START_ITEM = int(args.start)
    if args.end and int(args.end) > START_ITEM:
        END_ITEM = int(args.end)
    else:
        END_ITEM = None
    if args.storage:
        STORAGE_DIR = args.storage
    import gevent
    from gevent.pool import Pool
    from gevent.queue import Queue, Empty

    tasks = Queue(maxsize=THREAD_NUM * 3)
    logger.info("Thread count: %d, start item #: %d", THREAD_NUM, START_ITEM),
    pool = Pool(THREAD_NUM)

    object_iterator = itertools.islice(object_iterator, START_ITEM, END_ITEM)
    pool.spawn(producer, object_iterator)
    for worker_num in xrange(THREAD_NUM - 1):
        pool.spawn(consumer, worker_num)
    gevent.wait()
