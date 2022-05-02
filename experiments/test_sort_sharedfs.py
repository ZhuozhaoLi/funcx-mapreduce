import json
import sys
import time
import argparse
import sqlite3
import glob

from funcx.sdk.client import FuncXClient


def sampling(filename, num_reducers=10):
    import random
    with open(filename) as f:
        lines = random.sample(f.readlines(), num_reducers - 1)
    for i in range(len(lines)):
        lines[i] = lines[i].strip("\n")
    return sorted(lines)


def mapper_sharedfs(filename, keys, mapper_index=1, intermediate_path='.'):
    import pickle
    import time
    import string

    print("starting")
    start = time.time()
    with open(filename) as f:
        lines = f.readlines()
    end_reading = time.time()
    print(end_reading)
    buckets = {}
    for i in range(len(keys) + 1):
        buckets[i] = []
    
    for line in lines:
        line = line.strip("\n")
        if line < keys[0]:
            idx = 0
        elif line >= keys[-1]:
            idx = len(keys)
        else:
            for i in range(len(keys)-1):
                if line >= keys[i] and line < keys[i+1]:
                    idx = i + 1
                    break
        buckets[idx].append(line)
    end_processing = time.time()
    print(end_processing)

    output_path = "{}/reducer-{}-{}.txt"
    
    for q in buckets:
        output = output_path.format(intermediate_path, q, mapper_index)
        print("Output path: {}".format(output))
        with open(output, 'w') as f:
            for line in buckets[q]:
                f.write("{}\n".format(line))
    end = time.time()
    return start, end_reading, end_processing, end


def reducer_sharedfs(intermediate_path='.', output_path=".", inputs=None, reducer_index=1):
    import time
    import pickle
    import glob

    start = time.time()
    if not inputs:
        inputs = []
        intermediate_data = glob.glob("{}/reducer-{}-*.txt".format(intermediate_path, reducer_index))
        for data in intermediate_data:
            with open(data) as f:
                inputs.extend(f.readlines())
    end_reading = time.time()
    
    res = sorted(inputs)
    end_processing = time.time()

    output_path = "{}/reducer-{}.txt".format(output_path, reducer_index)
    with open(output_path, 'w') as f:
        for r in res:
            f.write("{}".format(r))
    end = time.time()
    return start, end_reading, end_processing, end


def mapreduce_sharedfs(fxc, ep_id, input_path, intermediate_path='.', output_path='.', 
                       keys=None, num_reducers=10):
    mapper_func_uuid = fxc.register_function(mapper_sharedfs,
                                             ep_id, # TODO: We do not need ep id here
                                             description="Mapper function for shared fs")

    reducer_func_uuid = fxc.register_function(reducer_sharedfs,
                                              ep_id, # TODO: We do not need ep id here
                                              description="Mapper function for shared fs")

    print("mapper function uuid: {}; reducer function uuid: {}".format(mapper_func_uuid, reducer_func_uuid))
    
    map_task_ids = []
    for i, p in enumerate(input_path):
        task = fxc.run(filename=p, keys=keys,
                       mapper_index=i,
                       intermediate_path=intermediate_path,
                       endpoint_id=ep_id,
                       function_id=mapper_func_uuid)
        map_task_ids.append(task)
    map_results, _ = wait_for_result(fxc, map_task_ids)
    print(map_results)
    print("Finished mapper tasks =======================================================")

    reduce_task_ids = []
    for i in range(num_reducers):
        task = fxc.run(reducer_index=i,
                       intermediate_path=intermediate_path,
                       output_path=output_path,
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
        time.sleep(60)
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
        app text)"""
    )

    if args.local:
        num_reducers = 10
        keys = sampling("/projects/APSDataAnalysis/zz/data/sort_input/part0000.txt", num_reducers=num_reducers)
        input1 = mapper_sharedfs("/projects/APSDataAnalysis/zz/data/sort_input/part0000.txt",
                              keys=keys,
                              intermediate_path="/projects/APSDataAnalysis/zz/data/sort_intermediate",
                              mapper_index=0)
        input2 = mapper_sharedfs("/projects/APSDataAnalysis/zz/data/sort_input/part0001.txt",
                              keys=keys,
                              intermediate_path="/projects/APSDataAnalysis/zz/data/sort_intermediate",
                              mapper_index=1)
        for i in range(num_reducers):
            res = reducer_sharedfs(intermediate_path="/projects/APSDataAnalysis/zz/data/sort_intermediate",
                                output_path="/projects/APSDataAnalysis/zz/data/sort_output",
                                reducer_index=i)
        #print(res)

    else:
        fxc = FuncXClient()
        fxc.throttling_enabled = False

        input_path = glob.glob("/projects/APSDataAnalysis/zz/data/sort_input/part0*")
        #input_path = [__file__, '/home/zzli/tmp/redis-funcx/experiments/test_sort_redis.py']
        intermediate_path = "/projects/APSDataAnalysis/zz/data/sort_intermediate"
        output_path = "/projects/APSDataAnalysis/zz/data/sort_output"
        clean_dir(intermediate_path)
        clean_dir(output_path)
        num_reducers = 50

        keys = sampling("/projects/APSDataAnalysis/zz/data/sort_input/sort_30GB.txt", num_reducers=num_reducers)
        print("Processing {} input files with {} reducers".format(len(input_path), num_reducers))
        start = time.time()
        map_res, reduce_res, latest_completion_time = mapreduce_sharedfs(fxc, args.endpoint,
                                                                         input_path, intermediate_path,
                                                                         output_path, keys=keys,
                                                                         num_reducers=num_reducers)
        for r in map_res:
            data = (r, 'mapper', 'sharedfs', 'sort')
            print("Inserting {}".format(str(data)))
            db.execute("""
                insert into
                tasks(time, task_type, tag, app)
                values (?, ?, ?, ?)""", data
            )
            db.commit()

        for r in reduce_res:
            data = (r, 'reducer', 'sharedfs', 'sort')
            print("Inserting {}".format(str(data)))
            db.execute("""
                insert into
                tasks(time, task_type, tag, app)
                values (?, ?, ?, ?)""", data
            )
            db.commit()
        print("The whole experiment takes {} seconds".format(latest_completion_time - start))