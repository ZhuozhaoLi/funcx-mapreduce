from mpi4py import MPI
import numpy as np
import argparse
import platform
import pickle
from redis_q import RedisQueue
import redis
import sys

import time
from funcx.serialize import FuncXSerializer


fxs = FuncXSerializer()


def avg(x):
    return sum(x)/len(x)

def process_numpy(serialized):
    data = deserialize(serialized_data)
    return serialize(data.sum())

def serialize(data):
    return fxs.serialize(data)

def deserialize(data):
    return fxs.deserialize(data)


if __name__ == "__main__":


    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--size", default='100',
                        help="Size of nd array")
    parser.add_argument("-n", "--ntimes", default='10',
                        help="Number of times to repeat")
    parser.add_argument("-r", "--redis", default='127.0.0.1',
                        help="redis host address")
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    print("Current process rank: {}, total ranks: {}".format(rank, size))

    # redis client
    rc = redis.StrictRedis(host=args.redis, port=6379, decode_responses=True)
    
    if rank == 0:
        # Raw numpy in process
        raw = np.random.rand(int(args.size), 1)

        start = time.time()
        
        serialized_data = serialize(raw)
        rc.hset('1', 'task', serialized_data)
        rc.rpush('task_list', '1')
        delta = time.time() - start
        print(f"[Rank:0, len={len(raw)}] Cum time to put: {delta*1000:8.3f}ms")

        x = rc.blpop('result_list', timeout=0)
        result_list, result_id = x
        result = rc.hget(result_id, 'result')
        delta = time.time() - start
        print(f"[Rank:0, len={len(raw)}] Time to send and receive results: {delta*1000:8.3f}ms")
        print(f"Got result: {result}, deserialized: {deserialize(result)}")

    else:

        x = rc.blpop('task_list', timeout=0)
        task_list, task_id = x
        serialized_data = rc.hget(task_id, 'task')
        print("[TASKS] Got task_id {}".format(task_id))
        start = time.time()

        serialized_result = process_numpy(serialized_data)
        delta = time.time() - start
        print(f"[Rank:0, len={args.size}] Time on worker process: {delta*1000:8.3f}ms")

        rc.rpush('result_list', '1')
        rc.hset('1', 'result', serialized_result)
        delta = time.time() - start
        print(f"[Rank:0, len={args.size}] Recv,deserialize,send: {delta*1000:8.3f}ms")

