1, 配置环境
    发行版 zookeeper-x.x.x/src/c
        ./configure
        make
        make install
    源码版 zookeeper
        ant compile-native
2, 启动 zkServer.sh

结构介绍 watcher
    https://www.cnblogs.com/haippy/archive/2013/02/21/2920241.html

vim zookeeper_connect.c
gcc zookeeper_connect.c -o connect -L/usr/local/lib/ -lzookeeper_mt



https://blog.csdn.net/yangzhen92/article/details/53248294
https://blog.csdn.net/breaksoftware/article/details/79393121
https://blog.csdn.net/lijinqi1987/article/details/78815194
http://www.cnblogs.com/haippy/archive/2013/02/24/2924570.html
https://github.com/wangdamingll/zookeeper_cplusplus













发行版中 zookeeper/src/c/
    mkdir build
    cd build
    cmake ../
    make -j2
以下这两个命令可以参看源码
    cli 
    load-gen 

