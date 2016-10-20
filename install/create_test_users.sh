#!/bin/bash
for i in `seq 1 500`;
do
    user=test$i
    id -u user
    if [ $? -eq 0 ]; then
	echo "user: "$user " already crated"
    else
	echo "user: "$user " does not exists creating it"
	adduser $user
    fi
done

