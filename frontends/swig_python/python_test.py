#!/usr/bin/env python
##########################################################################
# frontends/swig_python/python_test.py
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

import unittest
import threading
import sys

import thrill


class TryThread(threading.Thread):

    def __init__(self, **kwargs):
        threading.Thread.__init__(self, **kwargs)
        self.exception = None

    def run(self):
        try:
            threading.Thread.run(self)
        except Exception:
            self.exception = sys.exc_info()
            raise


def run_thrill_threads(num_threads, thread_func):
    # construct a local context mock network
    ctxs = thrill.PyContext.ConstructLoopback(num_threads, 1)

    # but then start python threads for each context
    threads = []
    for thrid in range(0, num_threads):
        t = TryThread(target=thread_func, args=(ctxs[thrid],))
        t.start()
        threads.append(t)

    # wait for computation to finish
    for thr in threads:
        thr.join()

    # check for exceptions
    for thr in threads:
        if thr.exception:
            raise Exception(thr.exception)


def run_tests(thread_func):
    for num_threads in [1, 2, 5]:
        run_thrill_threads(num_threads, thread_func)


class TestOperations(unittest.TestCase):

    def test_generate_allgather(self):

        def test(ctx):
            test_size = 1024

            dia1 = ctx.Generate(
                test_size, lambda x: [int(x), "hello %d" % (x)])
            self.assertEqual(dia1.Size(), test_size)

            check = [[int(x), "hello %d" % (x)] for x in range(0, test_size)]
            self.assertEqual(dia1.AllGather(), check)

        run_tests(test)

    def test_generate_map_allgather(self):

        def test(ctx):
            test_size = 1024

            dia1 = ctx.Generate(test_size, lambda x: int(x))
            self.assertEqual(dia1.Size(), test_size)

            dia2 = dia1.Map(lambda x: [int(x), "hello %d" % (x)])

            check = [[int(x), "hello %d" % (x)] for x in range(0, test_size)]
            self.assertEqual(dia2.Size(), test_size)
            self.assertEqual(dia2.AllGather(), check)

            dia3 = dia1.Map(lambda x: [int(x), "two %d" % (x)])

            check = [[int(x), "two %d" % (x)] for x in range(0, test_size)]
            self.assertEqual(dia3.Size(), test_size)
            self.assertEqual(dia3.AllGather(), check)

        run_tests(test)

    def test_distribute_map_filter_allgather(self):

        def test(ctx):
            test_size = 1024

            dia1 = ctx.Distribute([x * x for x in range(0, test_size)])
            self.assertEqual(dia1.Size(), test_size)

            dia2 = dia1.Map(lambda x: [int(x), "hello %d" % (x)])

            dia3 = dia2.Filter(lambda x: x[0] >= 16 and x[0] < 10000)

            check = [[int(x * x), "hello %d" % (x * x)]
                     for x in range(4, 100)]
            self.assertEqual(dia3.AllGather(), check)

        run_tests(test)

    def test_generate_sum(self):

        def test(ctx):
            test_size = 16

            dia1 = ctx.Generate(test_size, lambda x: int(x))

            self.assertEqual(dia1.Size(), test_size)

            sum = dia1.Sum(lambda x = 0, y = 0: (x + y))

            self.assertEqual(120, sum)

        run_tests(test)

    def my_generator(self, index):
        #print("generator at index", index)
        return (index, "hello at %d" % (index))

    def my_thread(self, ctx):
        print("thread in python, rank", ctx.my_rank())

        dia1 = ctx.Generate(50, lambda x: [int(x), x])
        dia2 = dia1.Map(lambda x: (x[0], x[1] + " mapped"))

        s = dia2.Size()
        print("Size:", s)
        self.assertEqual(s, 50)

        print("AllGather:", dia2.AllGather())

        dia3 = dia2.ReduceByKey(lambda x: x[0] % 10,
                                lambda x, y: (x + y))

        print("dia3.Size:", dia3.Size())
        print("dia3.AllGather:", dia3.AllGather())

        dia4 = dia3.Filter(lambda x: x[0] == 2)
        print("dia4.AllGather:", dia4.AllGather())

        #####

        dia5 = ctx.Distribute([2, 3, 5, 7, 11, 13, 17, 19])
        print("dia5.AllGather:", dia5.AllGather())

    def notest_operations(self):
        run_thrill_threads(4, self.my_thread)

if __name__ == '__main__':
    unittest.main()

##########################################################################
