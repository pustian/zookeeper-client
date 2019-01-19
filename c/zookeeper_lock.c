#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <zookeeper.h>
#include <zookeeper_log.h>

int init_watchctx(watchctx_t* ctx) {
    if (0 != pthread_cond_init(&ctx->cond, NULL)) {
        fprintf(stderr, "condition init error\n");
        return -1;
    }
 
    if (0 != pthread_mutex_init(&ctx->cond_lock, NULL)) {
        fprintf(stderr, "mutex init error\n");
        pthread_cond_destroy(&ctx->cond);
        return -2;
    }
 
    return 0;
}

zhandle_t* init() {
    const char* hosts_port = "192.168.1.97:2181,192.168.1.98:2181,192.168.1.99:2181";
    int timeout = 30000;
    zhandle_t* zh = NULL;
 
//    watchctx_t ctx;
//    if (0 != init_watchctx(&ctx)) {
//        return zh;
//    }
 
    zh = zookeeper_init(hosts_port, main_watcher, timeout, 0, &ctx, 0);
    if (zh == NULL) {
        fprintf(stderr, "Error when connecting to zookeeper servers...\n");
        pthread_cond_destroy(&ctx.cond);
        pthread_mutex_destroy(&ctx.cond_lock);
        return zh;
    }
 
//    struct timeval now;  
//    struct timespec outtime;     
//    gettimeofday(&now, NULL);  
//    outtime.tv_sec = now.tv_sec + 1;  
//    outtime.tv_nsec = now.tv_usec * 1000;
// 
//    pthread_mutex_lock(&ctx.cond_lock);
//    int wait_result = pthread_cond_timedwait(&ctx.cond, &ctx.cond_lock, &outtime);
//    pthread_mutex_unlock(&ctx.cond_lock);
//    
//    pthread_cond_destroy(&ctx.cond);
//    pthread_mutex_destroy(&ctx.cond_lock);
// 
//    if (0 != wait_result) {
//        fprintf(stderr, "Connecting to zookeeper servers timeout...\n");
//        zookeeper_close(zh);
//        zh = NULL;
//        return zh;
//    }
 
    return zh;
}
