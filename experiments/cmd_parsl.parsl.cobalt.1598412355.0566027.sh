export REDIS_SERVERS="7000;10.128.12.123;10.128.12.125;10.128.12.126;"
hostname -i | awk '{print }' >> /gpfs/mira-home/zzli/tmp/redis-funcx/experiments/tmp.out
funcx-manager --debug --max_workers=64 -c 1.0 --poll 10 --task_url=tcp://thetalogin4:54685 --result_url=tcp://thetalogin4:54612 --logdir=/home/zzli/.funcx/funcx-redis/worker_logs --block_id=1 --hb_period=15 --hb_threshold=60 --worker_mode=no_container --scheduler_mode=hard --worker_type=RAW 
