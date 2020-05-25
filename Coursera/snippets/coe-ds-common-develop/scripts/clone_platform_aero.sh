#! /bin/bash

vals="$(grep -A3 "$1:" "$3" | grep -v "$1:")"
un=$(echo "$vals" | grep ' *un:')
un=${un//*un: /}
pss=$(echo "$vals" | grep ' *pss:')
pss=${pss//*pss: /}
git clone --single-branch --branch ${2} https://${un}:${pss}@code.platform.aero/coe-ds/"$1".git
