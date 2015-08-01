import unittest
import threading

import c7a

class TestOperations(unittest.TestCase):

    def my_generator(self,index):
        #print("generator at index", index)
        return (index, "hello at %d" % (index));

    def my_thread(self,jm):
        ctx = c7a.Context(jm, 0)
        print("thread in python, rank", ctx.rank())

        dia1 = c7a.Generate(ctx, self.my_generator, 50)
        dia2 = dia1.Map(lambda x : (x[0], x[1] + " mapped"))

        s = dia2.Size()
        print("Size:", s)
        self.assertEqual(s, 50)

        print("AllGather:", dia2.AllGather())

        # TODO: change to % 10 when things work.
        dia3 = dia2.ReduceBy(lambda x : (x[0] + 10),
                             lambda x,y : x + y)

        print("dia3.Size:", dia3.Size())
        # TODO: ReduceNode cannot be executed twice!
        #print("dia3.AllGather:", dia3.AllGather())

    def test_operations(self):

        nthreads = 4

        obj = c7a.JobManager.ConstructLocalMesh(nthreads, 1)

        threads = []

        for thrid in range(0,nthreads):
            t = threading.Thread(target=self.my_thread, args=(obj[thrid],))
            t.start()
            threads.append(t)

        for thr in threads:
            thr.join()

        print("Done")

if __name__ == '__main__':
    unittest.main()
