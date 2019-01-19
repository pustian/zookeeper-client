#include <stdio.h>
#include <zookeeper/zookeeper.h>

/* 设置zookeeper host，其中host字符串格式为逗号隔开的IP:PORT对 */
static const char* hosts_port = "192.168.1.97:2181,192.168.1.98:2181,192.168.1.99:2181";
/* 设置客户端连接服务器超时时间，单位ms */
int timeout = 30000;

static char* type2String(int type) {
    char *str = NULL;
    if     (type == ZOO_CREATED_EVENT    ) str = "zoo_created_event    " ;
    else if(type == ZOO_DELETED_EVENT    ) str = "zoo_deleted_event    " ;
    else if(type == ZOO_CHANGED_EVENT    ) str = "zoo_changed_event    " ;
    else if(type == ZOO_CHILD_EVENT      ) str = "zoo_child_event      " ;
    else if(type == ZOO_SESSION_EVENT    ) str = "zoo_session_event    " ;
    else if(type == ZOO_NOTWATCHING_EVENT) str = "zoo_notwatching_event" ;
    else                                   str = "unknow event"          ;
    return str;
}

static char* state2String(int state) {
    char *str = NULL;
    if     ( state == ZOO_EXPIRED_SESSION_STATE) str = "zoo_expired_session_state" ; 
    else if( state == ZOO_AUTH_FAILED_STATE    ) str = "zoo_auth_failed_state    " ;
    else if( state == ZOO_CONNECTING_STATE     ) str = "zoo_connecting_state     " ;
    else if( state == ZOO_ASSOCIATING_STATE    ) str = "zoo_associating_state    " ;
    else if( state == ZOO_CONNECTED_STATE      ) str = "zoo_connected_state      " ;
    else                                         str = "unknow state"              ;
    return str;
}
void watcher_global(zhandle_t * zh, int type, int state, const char *path, void *watcherCtx) {
    printf("*** type=%s state=%s path=%s", type2String(type), state2String(state), path); 
}

int main() {
    printf("BEGIN======\n");
    /* 设置日志为调试级别 */
    zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
    /* 用于启用或停用quarum端点的随机化排序，通常仅在测试时使用。
    *  如果非0，使得client连接到quarum端按照被初始化的顺序。
    *  如果是0，zookeeper_init将变更端点顺序，使得client连接分布在更优的端点上。*/
    zoo_deterministic_conn_order(1);
    /* 初始化zookeeper句柄，传入全局监视器回调函数、超时、以及上下文信息 */
    zhandle_t* zkhandle = zookeeper_init(hosts_port, watcher_global, 30000, 0, "hello world", 0);
    if (zkhandle == NULL) {
        fprintf(stderr, "Error when connecting to zookeeper servers...\n");
        exit(EXIT_FAILURE);
    }
    printf("======END\n");
    zookeeper_close(zkhandle);
    return 0;
}

