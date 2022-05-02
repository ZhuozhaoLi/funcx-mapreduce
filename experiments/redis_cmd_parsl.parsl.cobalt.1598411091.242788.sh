hostname -i | awk '{print }' >> /gpfs/mira-home/zzli/tmp/redis-funcx/experiments/hostname.txt
rm -rf /tmp/nodes.conf
redis-server --cluster-enabled yes --bind 0.0.0.0 --port 7000 --cluster-config-file /tmp/nodes.conf --cluster-node-timeout 5000 --appendonly yes --appendfilename /projects/APSDataAnalysis/zz/ --daemonize no 
