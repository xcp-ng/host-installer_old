#!/bin/bash

/lib/udev/scsi_id.old -g -x -s $1 -d $2 -p0x80 | awk -F= '/ID_SERIAL/ {print $2}'
