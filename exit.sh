#!/bin/bash
#退出状态码，最大255 ，超过则进行模运算
#testing  the exit status
var1=10
var2=20
var3=$[ $var1 + var2 ]
echo The Answer is $var3
exit 5

