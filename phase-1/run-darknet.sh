#!/bin/bash

cd /home/ubuntu/darknet

# rundarknet.sh video > redirect to results

echo "Running darknet..."

Xvfb :1 & export DISPLAY=:1

./darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg yolov3-tiny.weights /home/ubuntu/videos/$1 > /home/ubuntu/videos/$1.txt
