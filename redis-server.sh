#!/bin/bash

hostname -i > /gpfs/mira-home/zzli/tmp/redis-funcx/h.txt
redis-server --bind 0.0.0.0
