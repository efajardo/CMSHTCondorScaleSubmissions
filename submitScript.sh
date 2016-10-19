#/bin/bash
# bash scripts that submits a condor job as user
# the user is the first argument, the sleep time is the second one
echo "user: "$1
echo "sleeptime: "$2
sites=$3
echo "sites: "$3
cpus=$4
echo "cpus: "$4
echo "memory: "$5
memory=$5
sudo -u $1 -s -- <<EOF
cd /dataScaleTests/logFiles
if [ ! -d $1 ];then
 mkdir $1
fi
condor_submit /dataScaleTests/submitfiles/simple.sub -append 'args =$2' -append '+DESIRED_Sites="$3"' -append 'request_cpus=$4' -append 'request_memory=$5'
EOF
exit
