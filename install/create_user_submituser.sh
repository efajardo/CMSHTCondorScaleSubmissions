#!/bin/bash
id -u submituser
if [ $? -eq 0 ]; then
    echo "submituser user already crated"
else
    echo "submituser user does not exist, adding it"
    adduser submituser
fi
sudoers_file="/etc/sudoers.d/10_cms_scale_tests_sudoers"
if [ -e $sudoers_file ]; then
    echo "file "$sudoers_file" already exists"
else
    echo "moving sudoers file " $sudoers_file 
    cp 10_cms_scale_tests_sudoers /etc/sudoers.d/
fi
