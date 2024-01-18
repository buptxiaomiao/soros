#! /usr/bin/bash

# shellcheck disable=SC2046
bash /root/github/soros/bash/ods.sh > /root/log/soros/ods_`date +"%F"`.log
bash /root/github/soros/bash/l1.sh > /root/log/soros/l1_`date +"%F"`.log