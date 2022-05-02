import redis
import argparse
import pickle
import numpy as np
import time
import json
import struct
from funcx.serialize import FuncXSerializer


def process_numpy(pkl_data):

    data = fxs.deserialize(pkl_data)
    return fxs.serialize(data.sum())


def serialize(a):
   print(a.shape)
   h, w = a.shape
   shape = struct.pack('>II',h,w)
   encoded = shape + a.tobytes()
   return encoded


def deserialize(encoded):
   h, w = struct.unpack('>II',encoded[:8])
   a = np.frombuffer(encoded, dtype=np.uint16, offset=8).reshape(h,w)
   return a


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--size", default='100',
                        help="Size of nd array")
    parser.add_argument("-n", "--ntimes", default='10',
                        help="Number of times to repeat")
    parser.add_argument("-r", "--redis", default='127.0.0.1',
                        help="redis host address")
    args = parser.parse_args()

    fxs = FuncXSerializer()

    # redis client
    rc = redis.StrictRedis(host=args.redis, port=6379, decode_responses=True) 

    raw = np.random.rand(int(args.size), 1)
    #raw = '1' * int(args.size)

    start = time.time()
    pkl_data = raw
    rc.hset('1', 'task', fxs.serialize(pkl_data))
    rc.rpush('task_list', '1')
    delta = time.time() - start
    print(f"[Rank:0, len={len(raw)}] Cum time to put: {delta*1000:8.3f}ms")

    x = rc.blpop('task_list', timeout=0)
    task_list, task_id = x
    print(f"task_list: {task_list}, task_id: {type(task_id)}")
    pkl_data = rc.hget(task_id, 'task')
    print("[TASKS] Got task_id {}".format(task_id))
    start = time.time()

    pkl_result = process_numpy(pkl_data)
    delta = time.time() - start
    print(f"[Rank:0, len={len(raw)}] Time on worker process: {delta*1000:8.3f}ms")

    rc.hset('1', 'result', pkl_result)
    delta = time.time() - start
    print(f"[Rank:0, len={len(raw)}] Recv,deserialize,send: {delta*1000:8.3f}ms")

    result = rc.hget('1', 'result')
    delta = time.time() - start
    print(f"[Rank:0, len={len(raw)}] Time to send and receive results: {delta*1000:8.3f}ms")
    print(f"Got result: {fxs.deserialize(result)}")

