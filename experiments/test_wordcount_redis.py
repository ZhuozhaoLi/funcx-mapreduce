import json
import sys
import time
import argparse
import sqlite3
import glob

from funcx.sdk.client import FuncXClient


def mapper_redis(filename, num_reducers=10, local=False):
    import json
    import hashlib
    from funcx.redis.client import get_redis_client
    import time
    import string

    print("starting")
    start = time.time()
    with open(filename) as f:
        lines = f.readlines()
    end_reading = time.time()
    print(end_reading)
    buckets = {}
    for i in range(num_reducers):
        buckets[i] = {}
    
    for line in lines:
        line = line.strip("\n")
        line = line.translate(str.maketrans("", "", string.punctuation))
        words = line.split()
        for word in words:
            if len(word) == 0:
                continue
            idx = int(hashlib.sha1(word.encode()).hexdigest(), 16) % num_reducers
            if word not in buckets[idx]:
                buckets[idx][word] = 0
            buckets[idx][word] += 1
    end_processing = time.time()
    print(end_processing)
    if local:
        return buckets

    rq = get_redis_client()
    for q in buckets:
        rq.put(str(q), json.dumps(buckets[q]))
    end = time.time()
    return start, end_reading, end_processing, end


def reducer_redis(output_path=".", inputs=None, reducer_index=1, num_mappers=10, local=False):
    import time
    import json
    from funcx.redis.client import get_redis_client

    start = time.time()
    if not inputs:
        inputs = []
        rq = get_redis_client()
        for _ in range(num_mappers):
            intermediate = json.loads(rq.get(str(reducer_index)))
            inputs.append(intermediate)
    end_reading = time.time()
    
    res = {}
    for inp in inputs:
        for k, v in inp.items():
            if k not in res:
                res[k] = 0
            res[k] += v
    end_processing = time.time()

    if local:
        return res

    output_path = "{}/reducer-{}.json".format(output_path, reducer_index)
    with open(output_path, 'w') as f:
        json.dump(res, f)
    end = time.time()
    return start, end_reading, end_processing, end


def test_env(event):
    import os
    #from funcx.redis.client import get_redis_client
    #rc = get_redis_client()
    #rc.put("01", {'a': 1, 'b': 2})
    #res = rc.get(timeout=1)
    return os.environ['REDIS_SERVERS']


def test_client(event):
    import os
    import json
    from funcx.redis.client import get_redis_client

    rc = get_redis_client()
    rc.put("01", json.dumps({'a': 1, 'b': 2}))
    res = json.loads(rc.get("01"))
    return os.environ['REDIS_SERVERS'], res


def test(fxc, ep_id):

    fn_uuid = fxc.register_function(test_client,
                                    ep_id, # TODO: We do not need ep id here
                                    description="New sum function defined without string spec")
    print("FN_UUID : ", fn_uuid)


    res = fxc.run([1,2,3,99], endpoint_id=ep_id, function_id=fn_uuid)
    print(res)
    while True:
        try:
            result = fxc.get_result(res)
            break
        except Exception as e:
            if "pending" not in str(e):
                raise
        time.sleep(2)
    print(result)


def mapreduce_redis(fxc, ep_id, input_path, output_path='.', num_reducers=10):
    mapper_func_uuid = fxc.register_function(mapper_redis,
                                             ep_id, # TODO: We do not need ep id here
                                             description="Mapper function for redis")

    reducer_func_uuid = fxc.register_function(reducer_redis,
                                              ep_id, # TODO: We do not need ep id here
                                              description="Mapper function for redis")

    print("mapper function uuid: {}; reducer function uuid: {}".format(mapper_func_uuid, reducer_func_uuid))

    map_task_ids = []
    for p in input_path:
        task = fxc.run(filename=p, num_reducers=num_reducers, endpoint_id=ep_id, function_id=mapper_func_uuid)
        map_task_ids.append(task)
    map_results, _ = wait_for_result(fxc, map_task_ids)
    print(map_results)
    print("Finished mapper tasks =======================================================")

    reduce_task_ids = []
    for i in range(num_reducers):
        task = fxc.run(reducer_index=i, output_path=output_path, 
                       num_mappers=len(input_path),
                       endpoint_id=ep_id, function_id=reducer_func_uuid)
        reduce_task_ids.append(task)
    reduce_results, latest_completion_time = wait_for_result(fxc, reduce_task_ids)
    print(reduce_results)
    print("Finished reducer tasks =======================================================")
    return map_results, reduce_results, latest_completion_time


def wait_for_result(fxc, task_ids):
    while True:
        x = fxc.get_batch_status(task_ids)
        print(x)
        complete_count = sum([ 1 for t in task_ids if t in x and not x[t].get('pending', True) ])
        print("Batch status : {}/{} complete".format(complete_count, len(task_ids)))
        if complete_count == len(task_ids):
            print(x)
            results = []
            latest_completion_time = 0
            for task_id in x:
                if 'exception' in x[task_id]:
                    try:
                        x[task_id]['exception'].reraise()
                    except Exception as e:
                        print(e)
                        pass
                elif 'result' in x[task_id]:
                    res = ";".join(str(t) for t in x[task_id]['result'])
                    results.append(res)
                latest_completion_time = max(latest_completion_time, float(x[task_id]['completion_t']))
            break
        time.sleep(10)
    return results, latest_completion_time


def clean_dir(path):
    import shutil
    import os
    
    shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)
    print("Cleaned directory {}".format(path))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--endpoint", default='4d775982-54d6-4353-bb5e-90f670861c79')
    parser.add_argument("-l", "--local", action='store_true', help="run on local or not")
    args = parser.parse_args()

    db = sqlite3.connect('data.db')
    db.execute("""create table if not exists tasks(
        time text,
        task_type text,
        tag text,
        app)"""
    )

    if args.local:
        
        input1 = mapper_redis("/projects/APSDataAnalysis/zz/data/wordcount_input/part0001.txt", local=True, num_reducers=1)
        # input2 = mapper_redis("test_sort.py", local=True, num_reducers=1)
        print(input1)
        #print(input2)
        res = reducer_redis(inputs=[input1[0]], local=True)
        print(res)

    else:
        fxc = FuncXClient()
        fxc.throttling_enabled = False

        # test(fxc, args.endpoint)
        input_path = [__file__, '/home/zzli/tmp/redis-funcx/experiments/test_sort_redis.py']
        # output_path = "/home/zzli/tmp/redis-funcx/experiments"
        #input_path = ["/projects/APSDataAnalysis/zz/data/wikipedia_50GB/file90", "/projects/APSDataAnalysis/zz/data/wikipedia_50GB/file99"]
        #output_path = "/projects/APSDataAnalysis/zz/data/wikipedia_50GB"

        start = time.time()
        input_path = glob.glob("/projects/APSDataAnalysis/zz/data/wordcount_input/part*")
        output_path = "/projects/APSDataAnalysis/zz/data/wordcount_output"
        clean_dir(output_path)
        num_reducers = 20
        print("Processing {} input files with {} reducers".format(len(input_path), num_reducers))
        map_res, reduce_res, latest_completion_time = mapreduce_redis(fxc, args.endpoint, input_path, output_path, num_reducers=num_reducers)

        for r in map_res:
            data = (r, 'mapper', 'redis', 'wordcount')
            print("Inserting {}".format(str(data)))
            db.execute("""
                insert into
                tasks(time, task_type, tag, app)
                values (?, ?, ?, ?)""", data
            )
            db.commit()

        for r in reduce_res:
            data = (r, 'reducer', 'redis', 'wordcount')
            print("Inserting {}".format(str(data)))
            db.execute("""
                insert into
                tasks(time, task_type, tag, app)
                values (?, ?, ?, ?)""", data
            )
            db.commit()
        print("The whole experiment takes {} seconds".format(latest_completion_time - start))