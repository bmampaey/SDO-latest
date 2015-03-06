#!/bin/bash
if ! ps -eaf | grep make_latest_videos_and_images | grep python >/dev/null ; then 
nohup /home/sdo/latest/make_latest_videos_and_images.py > /home/sdo/latest/make_latest_videos_and_images.out &
echo "Restarted the make_latest_videos_and_images daemon";
fi
