from funcx.config import Config
from parsl.providers import LocalProvider
from parsl.executors import HighThroughputExecutor
from funcx.providers import CobaltProvider
from funcx.launchers import AprunLauncher
from parsl.addresses import address_by_hostname
from funcx.strategies import SimpleStrategy
from funcx.redis.redis_cluster import RedisCluster


config = Config(
    provider=CobaltProvider(
        #queue='debug-flat-quad',
        #account='CSC249ADCD01',
        #account='APSDataAnalysis',
        account='CVD_Research',
        queue='CVD_Research',
        launcher=AprunLauncher(overrides="-d 64"),
        walltime='03:00:00',
        nodes_per_block=8,
        max_blocks=1,
        min_blocks=1,
        init_blocks=1,
        scheduler_options='',
        worker_init='source ~/anaconda3/etc/profile.d/conda.sh;conda activate funcx-redis',
        cmd_timeout=600,
    ),
    redisCluster=RedisCluster(
        nodes=3,
    ),
    scaling_enabled=True,
    worker_debug=True,
    scheduler_mode='hard',
    heartbeat_period=15,
    heartbeat_threshold=60,
    max_workers_per_node=64,
    strategy=SimpleStrategy(max_idletime=600),
)

