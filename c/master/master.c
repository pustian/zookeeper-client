
#include "master.h"
 
/*
 * 辅助函数
 */
char * make_path(int num, ...) {
    const char * tmp_string;
    va_list arguments;
    va_start ( arguments, num );
    int total_length = 0;
    int x;
    for ( x = 0; x < num; x++ ) {
        tmp_string = va_arg ( arguments, const char * );
        if(tmp_string != NULL) {
            printf(("Counting path with this path %s (%d)", tmp_string, num));
            total_length += strlen(tmp_string);
        }
    }
 
    va_end ( arguments );
 
    char * path = malloc(total_length * sizeof(char) + 1);
    path[0] = '\0';
    va_start ( arguments, num );
    
    for ( x = 0; x < num; x++ ) {
        tmp_string = va_arg ( arguments, const char * );
        if(tmp_string != NULL) {
            printf(("Counting path with this path %s",
                       tmp_string));
            strcat(path, tmp_string);
        }
    }
 
    return path;
}
 
struct String_vector* make_copy( const struct String_vector* vector ) {
    struct String_vector* tmp_vector = malloc(sizeof(struct String_vector));
    
    tmp_vector->data = malloc(vector->count * sizeof(const char *));
    tmp_vector->count = vector->count;
    
    int i;
    for( i = 0; i < vector->count; i++) {
        tmp_vector->data[i] = strdup(vector->data[i]);
    }
    
    return tmp_vector;
}
 
 
/*
 * 分配String_vector，从zookeeper.jute.c复制
 */
int allocate_vector(struct String_vector *v, int32_t len) {
    if (!len) {
        v->count = 0;
        v->data = 0;
    } else {
        v->count = len;
        v->data = calloc(sizeof(*v->data), len);
    }
    return 0;
}
 
 
/*
 * 释放内存的函数
 */
void free_vector(struct String_vector* vector) {
    int i;
    
    // Free each string
    for(i = 0; i < vector->count; i++) {
        free(vector->data[i]);
    }
    
    // Free data
    free(vector -> data);
    
    // Free the struct
    free(vector);
}
 
void free_task_info(struct task_info* task) {
    free(task->name);
    free(task->value);
    free(task->worker);
    free(task);
}
 
/*
 * 处理workers和任务缓存的函数
 */
 
int contains(const char * child, const struct String_vector* children) {
  int i;
  for(i = 0; i < children->count; i++) {
    if(!strcmp(child, children->data[i])) {
      return 1;
    }
  }
 
  return 0;
}
 
/*
 * 此函数返回当前与之前相比是新的元素，并更新之前的元素
 */
struct String_vector* added_and_set(const struct String_vector* current,
                                    struct String_vector** previous) {
    struct String_vector* diff = malloc(sizeof(struct String_vector));
    
    int count = 0;
    int i;
    for(i = 0; i < current->count; i++) {
        if (!contains(current->data[i], (*previous))) {
            count++;
        }
    }
    
    allocate_vector(diff, count);
    
    int prev_count = count;
    count = 0;
    for(i = 0; i < current->count; i++) {
        if (!contains(current->data[i], (* previous))) {
            diff->data[count] = malloc(sizeof(char) * strlen(current->data[i]) + 1);
            memcpy(diff->data[count++],
                   current->data[i],
                   strlen(current->data[i]));
        }
    }
    
    assert(prev_count == count);
    
    free_vector((struct String_vector*) *previous);
    (*previous) = make_copy(current);
    
    return diff;
    
}
 
/*
 * 此函数返回与之前想比较已被删除的元素，并更新之前的元素
 */
struct String_vector* removed_and_set(const struct String_vector* current,
                                      struct String_vector** previous) {
    
    struct String_vector* diff = malloc(sizeof(struct String_vector));
    
    int count = 0;
    int i;
    for(i = 0; i < (* previous)->count; i++) {
        if (!contains((* previous)->data[i], current)) {
            count++;
        }
    }
    
    allocate_vector(diff, count);
    
    int prev_count = count;
    count = 0;
    for(i = 0; i < (* previous)->count; i++) {
        if (!contains((* previous)->data[i], current)) {
            diff->data[count] = malloc(sizeof(char) * strlen((* previous)->data[i]));
            strcpy(diff->data[count++], (* previous)->data[i]);
        }
    }
 
    assert(prev_count == count);
 
    free_vector((struct String_vector*) *previous);
    (*previous) = make_copy(current);
    
    return diff;
}
 
/*
 * 辅助函数结束, 以上都与zookeeper相关
 */
 
/**
 * 我们使用Watcher来处理会话事件。特别是，当它收到一个ZOO_CONNECTED_STATE事件时
 * 我们设置连接变量，以便知道连接已建立
 */
void main_watcher (zhandle_t *zkh,
                   int type,
                   int state,
                   const char *path,
                   void* context)
{
    /*
     * zookeeper_init 可能没有返回，所以我们使用zkh代替
     */
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            connected = 1;
            printf(("Received a connected event."));
        } else if (state == ZOO_CONNECTING_STATE) {
            if(connected == 1) {
                printf(("Disconnected."));
            }
            connected = 0;
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            expired = 1;
            connected = 0;
            zookeeper_close(zkh);
        }
    }
    printf(("Event: %s, %d", type2string(type), state));
}
 
int is_connected() {
    return connected;
}
 
int is_expired() {
    return expired;
}
 
/**
 *
 * 分配任务，但首先读取任务数据。在这个简单的实现中，
 * 在znode中没有真正的任务数据，但是我们包含了获取数据的例子
 */
void assign_tasks(const struct String_vector *strings) {
    /*
     * For each task, assign it to a worker.
     */
    printf(("Task count: %d", strings->count));
    int i;
    for( i = 0; i < strings->count; i++) {
        printf(("Assigning task %s",
                   (char *) strings->data[i]));
        get_task_data( strings->data[i] );
    }
}
 
 
/**
 *
 * 在调用获取任务列表的时候调用完成函数
 *
 */
void tasks_completion (int rc,
                       const struct String_vector *strings,
                       const void *data) {
    switch (rc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            get_tasks();
            
            break;
            
        case ZOK:
            printf(("Assigning tasks"));
 
            struct String_vector *tmp_tasks = added_and_set(strings, &tasks);
            assign_tasks(tmp_tasks);
            free_vector(tmp_tasks);
 
            break;
        default:
            printf(("Something went wrong when checking tasks: %s", rc2string(rc)));
 
            break;
    }
}
 
/**
 *
 * 当任务列表更改时，调用watcher函数
 *
 */
void tasks_watcher (zhandle_t *zh,
                    int type,
                    int state,
                    const char *path,
                    void *watcherCtx) {
    printf(("Tasks watcher triggered %s %d", path, state));
    if( type == ZOO_CHILD_EVENT) {
        assert( !strcmp(path, "/tasks") );
        get_tasks();
    } else {
        printf(("Watched event: %s", type2string(type)));
    }
    printf(("Tasks watcher done"));
}
 
static int task_count = 0;
 
/**
 *
 * 获取任务列表
 *
 */
void get_tasks () {
  printf(("Getting tasks"));
    zoo_awget_children(zh,
                       "/tasks",
                       tasks_watcher,
                       NULL,
                       tasks_completion,
                       NULL);
}
 
/**
 *
 * 在删除任务znode的请求返回时调用完成函数
 *
 */
void delete_task_completion(int rc, const void *data) {
    switch (rc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            delete_pending_task((const char *) data);
            
            break;
        case ZOK:
            printf(("Deleted task: %s", (char *) data));
            free((char *) data);
            
            break;
        default:
            printf(("Something went wrong when deleting task: %s",
                       rc2string(rc)));
 
            break;
    }
}
 
/**
 *
 * 一旦分配完成，删除暂挂任务
 *
 */
void delete_pending_task (const char * path) {
    if(path == NULL) return;
    
    char * tmp_path = strdup(path);
    zoo_adelete(zh,
                tmp_path,
                -1,
                delete_task_completion,
                (const void*) tmp_path);
}
 
 
 
void workers_completion (int rc,
                         const struct String_vector *strings,
                         const void *data) {
    switch (rc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            get_workers();
            
            break;
        case ZOK:
            if(strings->count == 1) {
                printf(("Got %d worker", strings->count));
            } else {
                printf(("Got %d workers", strings->count));
            }
 
            struct String_vector *tmp_workers = removed_and_set(strings, &workers);
            free_vector(tmp_workers);
            get_tasks();
 
            break;
        default:
            printf(("Something went wrong when checking workers: %s", rc2string(rc)));
            
            break;
    }
}
 
void workers_watcher (zhandle_t *zh, int type, int state, const char *path,void *watcherCtx) {
    if( type == ZOO_CHILD_EVENT) {
        assert( !strcmp(path, "/workers") );
        get_workers();
    } else {
        printf(("Watched event: ", type2string(type)));
    }
}
 
void get_workers() {
    zoo_awget_children(zh,
                       "/workers",
                       workers_watcher,
                       NULL,
                       workers_completion,
                       NULL);
}
 
void take_leadership() {
    get_workers();
}
 
/*
 * 在尝试 创建master lock时发生错误，
 * 我们需要检查znode是否存在并确认其内容
 */
 
void master_check_completion (int rc, const char *value, int value_len,
                              const struct Stat *stat, const void *data) {
    int master_id;
    switch (rc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            check_master();
            
            break;
        case ZOK:
            sscanf(value, "%x", &master_id );
            if(master_id == server_id) {
                take_leadership();
                printf(("Elected primary master"));
            } else {
                master_exists();
                printf(("The primary is some other process"));
            }
            
            break;
        case ZNONODE:
            run_for_master();
            
            break;
        default:
        printf(("Something went wrong when checking the master lock: %s", rc2string(rc)));
            
            break;
    }
}
 
void check_master () {
    zoo_aget(zh,
             "/master",
             0,
             master_check_completion,
             NULL);
}
 
void task_assignment_completion (int rc, const char *value, const void *data) {
    switch (rc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            task_assignment((struct task_info*) data);
            
            break;
        case ZOK:
            if(data != NULL) {
                /*
                 * 从挂起列表中删除任务
                 */
                printf(("Deleting pending task %s",
                           ((struct task_info*) data)->name));
                char * del_path = "";
                del_path = make_path(2, "/tasks/", ((struct task_info*) data)->name);
                if(del_path != NULL) {
                    delete_pending_task(del_path);
                }
                free(del_path);
                free_task_info((struct task_info*) data);
            }
 
            break;
        case ZNODEEXISTS:
            printf(("Assignment has already been created: %s", value));
 
            break;
        default:
            printf(("Something went wrong when checking assignment completion: %s", rc2string(rc)));
 
            break;
    }
}
 
void task_assignment(struct task_info *task){
    //将任务添加到工作列表
    char * path = make_path(4, "/assign/" , task->worker, "/", task->name);
    zoo_acreate(zh,
                path,
                task->value,
                task->value_len,
                &ZOO_OPEN_ACL_UNSAFE,
                0,
                task_assignment_completion,
                (const void*) task);
    free(path);
}
 
void get_task_data_completion(int rc, const char *value, int value_len,
                              const struct Stat *stat, const void *data) {
    int worker_index;
    
    switch (rc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            get_task_data((const char *) data);
            
            break;
            
        case ZOK:
            printf(("Choosing worker for task %s", (const char *) data));
            if(workers != NULL) {
                /*
                 * Choose worker
                 */
                worker_index = (rand() % workers->count);
                printf(("Chosen worker %d %d",
                           worker_index, workers->count));
            
                /*
                 *分配任务给worker
                 */
                struct task_info *new_task;
                new_task = (struct task_info*) malloc(sizeof(struct task_info));
            
                new_task->name = (char *) data;
                new_task->value = strndup(value, value_len);
                new_task->value_len = value_len;
 
                const char * worker_string = workers->data[worker_index];
                new_task->worker = strdup(worker_string);
            
                printf(("Ready to assign it %d, %s",
                           worker_index,
                           workers->data[worker_index]));
                task_assignment(new_task);
            }
 
            break;
        default:
            printf(("Something went wrong when checking the master lock: %s",
                       rc2string(rc)));
            
            break;
    }
}
 
void get_task_data(const char *task) {
    if(task == NULL) return;
    
    printf(("Task path: %s",
               task));
    char * tmp_task = strndup(task, 15);
    char * path = make_path(2, "/tasks/", tmp_task);
    printf(("Getting task data %s",
               tmp_task));
    
    zoo_aget(zh,
             path,
             0,
             get_task_data_completion,
             (const void *) tmp_task);
    free(path);
}
 
 
 
/*
 * Run for master.
 */
 
 
void run_for_master();
 
void master_exists_watcher (zhandle_t *zh,
                            int type,
                            int state,
                            const char *path,
                            void *watcherCtx) {
    if( type == ZOO_DELETED_EVENT) {
        assert( !strcmp(path, "/master") );
        run_for_master();
    } else {
        printf(("Watched event: ", type2string(type)));
    }
}
 
void master_exists_completion (int rc, const struct Stat *stat, const void *data) {
    switch (rc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            master_exists();
            
            break;
            
        case ZOK:
            break;
        case ZNONODE:
            printf(("Previous master is gone, running for master"));
            run_for_master();
 
            break;
        default:
            printf(("Something went wrong when executing exists: %s", rc2string(rc)));
 
            break;
    }
}
 
void master_exists() {
    zoo_awexists(zh,
                 "/master",
                 master_exists_watcher,
                 NULL,
                 master_exists_completion,
                 NULL);
}
 
void master_create_completion (int rc, const char *value, const void *data) {
    switch (rc) {
        case ZCONNECTIONLOSS:
            check_master();
            
            break;
            
        case ZOK:
            take_leadership();
            
            break;
            
        case ZNODEEXISTS:
            master_exists();
            
            break;
            
        default:
            printf(("Something went wrong when running for master."));
 
            break;
    }
}
 
void run_for_master() {
    if(!connected) {
        printf(("Client not connected to ZooKeeper"));
 
        return;
    }
    
    char server_id_string[9];
    snprintf(server_id_string, 9, "%x", server_id);
    zoo_acreate(zh,
                "/master",
                (const char *) server_id_string,
                strlen(server_id_string) + 1,
                &ZOO_OPEN_ACL_UNSAFE,
                ZOO_EPHEMERAL,
                master_create_completion,
                NULL);
}
 
/*
 * 创建父节点
 */
 
void create_parent_completion (int rc, const char * value, const void * data) {
    switch (rc) {
        case ZCONNECTIONLOSS:
            create_parent(value, (const char *) data);
            
            break;
        case ZOK:
            printf(("Created parent node", value));
 
            break;
        case ZNODEEXISTS:
            printf(("Node already exists"));
            
            break;
        default:
      printf(("Something went wrong when running for master: %s, %s", value, rc2string(rc)));
            
            break;
    }
}
 
void create_parent(const char * path, const char * value) {
    zoo_acreate(zh,
                path,
                value,
                0,
                &ZOO_OPEN_ACL_UNSAFE,
                0,
                create_parent_completion,
                NULL);
    
}
 
void bootstrap() {
    if(!connected) {
      printf(("Client not connected to ZooKeeper"));
        return;
    }
    
    create_parent("/workers", "");
    create_parent("/assign", "");
    create_parent("/tasks", "");
    create_parent("/status", "");
    
    // Initialize tasks
    tasks = malloc(sizeof(struct String_vector));
    allocate_vector(tasks, 0);
    workers = malloc(sizeof(struct String_vector));
    allocate_vector(workers, 0);
}
 
int init (char * hostPort) {
    srand(time(NULL));
    server_id  = rand();
    
    zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
    
    zh = zookeeper_init(hostPort,
                        main_watcher,
                        15000,
                        0,
                        0,
                        0);
    
    return errno;
}
 
int main (int argc, char * argv[]) {
    printf(("THREADED defined"));
    if (argc != 2) {
        fprintf(stderr, "USAGE: %s host:port\n", argv[0]);
        exit(1);
    }
    
    /*
     * 初始化zookeeper session
     */
    if(init(argv[1])){
        printf(("Error while initializing the master: ", errno));
    }
    
#ifdef THREADED
    /*
     * Wait until connected
     */
    while(!is_connected()) {
        sleep(1);
    }
    
    printf(("Connected, going to bootstrap and run for master"));
    
    /*
     * Create parent znodes
     */
    bootstrap();
    
    /*
     * Run for master
     */
    run_for_master();
    
    /*
     * 运行直到会话过期
     */
    
    while(!is_expired()) {
        sleep(1);
    }
#else
    int run = 0;
    fd_set rfds, wfds, efds;
 
    FD_ZERO(&rfds);
    FD_ZERO(&wfds);
    FD_ZERO(&efds);
    while (!is_expired()) {
        int fd = -1;
        int interest = 0;
        int events ;
        struct timeval tv;
        int rc;
        
        zookeeper_interest(zh, &fd, &interest, &tv);
        if (fd != -1) {
            if (interest&ZOOKEEPER_READ) {
                FD_SET(fd, &rfds);
            } else {
                FD_CLR(fd, &rfds);
            }
            if (interest&ZOOKEEPER_WRITE) {
                FD_SET(fd, &wfds);
            } else {
                FD_CLR(fd, &wfds);
            }
        } else {
            fd = 0;
        }
        
        /*
         * 下一个if块包含调用bootstrap、运行master
         * 只有当客户端已建立好连接并且is_connected为true
         * 的情况下才会进入
         */
        if(is_connected() && !run) {
            printf(("Connected, going to bootstrap and run for master"));
            
            /*
             * Create parent znodes
             */
            bootstrap();
            
            /*
             * Run for master
             */
            run_for_master();
            
            run = 1;
        }
        
        rc = select(fd+1, &rfds, &wfds, &efds, &tv);
        events = 0;
        if (rc > 0) {
            if (FD_ISSET(fd, &rfds)) {
                events |= ZOOKEEPER_READ;
            }
            if (FD_ISSET(fd, &wfds)) {
                events |= ZOOKEEPER_WRITE;
            }
        }
        
        zookeeper_process(zh, events);
    }
#endif
    
    return 0; 
}

