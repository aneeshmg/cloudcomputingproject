#!/bin/bash

cd /home/ubuntu/darknet

# rundarknet.sh video > redirect to results

./darknet detector $1 cfg/coco.data cfg/yolov3-tiny.cfg yolov3-tiny.weights /home/ubuntu/videos/$1 > /home/ubuntu/videos/$1.txt
