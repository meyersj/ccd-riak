#!/bin/bash

# total lines = 17908260
# with 3 files each has 5969420 lines

start1=1
end1=5969420
start2=5969421
end2=11938840
start3=11938841
end3=17908260


for i in 1 2 3;
do
    start=start${i}
    end=end${i}
    echo ${!start} ${!end}
    cat loopdata_headers > loopdata${i}.csv
    sed -n ${!start},${!end}p loopdata.csv >> loopdata${i}.csv
done
