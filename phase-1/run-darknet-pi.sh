#!/bin/bash

cd /home/pi/darknet

# rundarknet.sh video > redirect to results

echo "Running darknet..."

Xvfb :1 & export DISPLAY=:1

./darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg yolov3-tiny.weights /home/pi/temp_videos/$1 > /home/pi/temp_videos/$1.txt
