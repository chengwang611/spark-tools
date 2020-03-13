#!/bin/bash
# PASSWORD IS CREATED BY  : "mypassword" | openssl enc -base64 > password.dat
PASSWORD=$(cat /home/hadoop/password.dat | openssl enc -base64 -d)
echo "password is:$PASSWORD"