#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

/* --- Configuration & Constants --- */
#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 64
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

/* --- Enums & Structs --- */
typedef enum { CMD_SUPERVISOR = 0, CMD_START, CMD_RUN, CMD_PS, CMD_LOGS, CMD_STOP } command_kind_t;
typedef enum { CONTAINER_STARTING, CONTAINER_RUNNING, CONTAINER_STOPPED, CONTAINER_EXITED } container_state_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head, tail, count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty, not_full;
} bounded_buffer_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    container_state_t state;
    time_t started_at;
    struct container_record *next;
} container_record_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[256];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
} control_request_t;

typedef struct {
    int status;
    char message[256];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[256];
    int pipe_fd; // For log redirection
} child_config_t;

/* --- Supervisor Context --- */
typedef struct {
    int monitor_fd;
    int server_fd;
    bounded_buffer_t log_buffer;
    container_record_t *containers;
    pthread_mutex_t metadata_lock;
} supervisor_ctx_t;

supervisor_ctx_t ctx;

/* --- Bounded Buffer Logic --- */
void bb_init(bounded_buffer_t *bb) {
    memset(bb, 0, sizeof(*bb));
    pthread_mutex_init(&bb->mutex, NULL);
    pthread_cond_init(&bb->not_empty, NULL);
    pthread_cond_init(&bb->not_full, NULL);
}

int bb_push(bounded_buffer_t *bb, const log_item_t *item) {
    pthread_mutex_lock(&bb->mutex);
    while (bb->count == LOG_BUFFER_CAPACITY && !bb->shutting_down)
        pthread_cond_wait(&bb->not_full, &bb->mutex);
    if (bb->shutting_down) { pthread_mutex_unlock(&bb->mutex); return -1; }
    bb->items[bb->tail] = *item;
    bb->tail = (bb->tail + 1) % LOG_BUFFER_CAPACITY;
    bb->count++;
    pthread_cond_signal(&bb->not_empty);
    pthread_mutex_unlock(&bb->mutex);
    return 0;
}

int bb_pop(bounded_buffer_t *bb, log_item_t *item) {
    pthread_mutex_lock(&bb->mutex);
    while (bb->count == 0 && !bb->shutting_down)
        pthread_cond_wait(&bb->not_empty, &bb->mutex);
    if (bb->count == 0 && bb->shutting_down) { pthread_mutex_unlock(&bb->mutex); return -1; }
    *item = bb->items[bb->head];
    bb->head = (bb->head + 1) % LOG_BUFFER_CAPACITY;
    bb->count--;
    pthread_cond_signal(&bb->not_full);
    pthread_mutex_unlock(&bb->mutex);
    return 0;
}

/* --- Logging Thread & Producers --- */
void *logger_thread_func(void *arg) {
    log_item_t item;
    while (bb_pop(&ctx.log_buffer, &item) == 0) {
        char log_path[PATH_MAX];
        snprintf(log_path, PATH_MAX, "%s/%s.log", LOG_DIR, item.container_id);
        int fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0) {
            write(fd, item.data, item.length);
            close(fd);
        }
    }
    return NULL;
}

void *log_producer_func(void *arg) {
    child_config_t *cfg = (child_config_t *)arg;
    char buffer[LOG_CHUNK_SIZE];
    ssize_t n;
    while ((n = read(cfg->pipe_fd, buffer, sizeof(buffer))) > 0) {
        log_item_t item;
        strncpy(item.container_id, cfg->id, CONTAINER_ID_LEN);
        item.length = n;
        memcpy(item.data, buffer, n);
        bb_push(&ctx.log_buffer, &item);
    }
    close(cfg->pipe_fd);
    free(cfg);
    return NULL;
}

/* --- Container Child Logic --- */
int child_entry(void *arg) {
    child_config_t *cfg = (child_config_t *)arg;

    // Isolate UTS (Hostname)
    sethostname(cfg->id, strlen(cfg->id));

    // Redirect stdout/stderr to the supervisor pipe
    dup2(cfg->pipe_fd, STDOUT_FILENO);
    dup2(cfg->pipe_fd, STDERR_FILENO);
    close(cfg->pipe_fd);

    // Mount and Chroot logic
    if (mount(cfg->rootfs, cfg->rootfs, NULL, MS_BIND | MS_REC, NULL) < 0) exit(1);
    if (chroot(cfg->rootfs) < 0 || chdir("/") < 0) exit(1);
    
    mount("proc", "/proc", "proc", 0, NULL);

    char *exec_args[] = {"/bin/sh", "-c", cfg->command, NULL};
    execvp(exec_args[0], exec_args);
    
    perror("Exec failed");
    return 1;
}

/* --- Signal Handling (Reaping) --- */
void sigchld_handler(int sig) {
    int status;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx.metadata_lock);
        container_record_t *curr = ctx.containers;
        while (curr) {
            if (curr->host_pid == pid) {
                curr->state = CONTAINER_EXITED;
                break;
            }
            curr = curr->next;
        }
        pthread_mutex_unlock(&ctx.metadata_lock);
    }
}

/* --- Supervisor Event Loop --- */
int run_supervisor(const char *base_rootfs) {
    printf("[Supervisor] Initializing on base: %s\n", base_rootfs);
    mkdir(LOG_DIR, 0755);
    bb_init(&ctx.log_buffer);
    pthread_mutex_init(&ctx.metadata_lock, NULL);
    
    signal(SIGCHLD, sigchld_handler);
    pthread_t logger_tid;
    pthread_create(&logger_tid, NULL, logger_thread_func, NULL);

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un addr = { .sun_family = AF_UNIX };
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path)-1);
    unlink(CONTROL_PATH);
    
    if (bind(ctx.server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("Bind failed");
        return 1;
    }
    listen(ctx.server_fd, 10);

    while (1) {
        int cfd = accept(ctx.server_fd, NULL, NULL);
        if (cfd < 0) continue;

        control_request_t req;
        read(cfd, &req, sizeof(req));
        control_response_t res = {0, "OK"};

        if (req.kind == CMD_RUN || req.kind == CMD_START) {
            int pipefds[2];
            pipe(pipefds);

            child_config_t *cfg = malloc(sizeof(child_config_t));
            strncpy(cfg->id, req.container_id, CONTAINER_ID_LEN);
            strncpy(cfg->rootfs, req.rootfs, PATH_MAX);
            strncpy(cfg->command, req.command, 255);
            cfg->pipe_fd = pipefds[1];

            void *stack = malloc(STACK_SIZE);
            pid_t pid = clone(child_entry, stack + STACK_SIZE, 
                              CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWUTS | SIGCHLD, cfg);

            if (pid > 0) {
                pthread_mutex_lock(&ctx.metadata_lock);
                container_record_t *rec = malloc(sizeof(container_record_t));
                strncpy(rec->id, req.container_id, CONTAINER_ID_LEN);
                rec->host_pid = pid;
                rec->state = CONTAINER_RUNNING;
                rec->started_at = time(NULL);
                rec->next = ctx.containers;
                ctx.containers = rec;
                pthread_mutex_unlock(&ctx.metadata_lock);

                // Start producer thread to read container output
                cfg->pipe_fd = pipefds[0]; 
                close(pipefds[1]);
                pthread_t prod_tid;
                pthread_create(&prod_tid, NULL, log_producer_func, cfg);
                snprintf(res.message, 256, "Launched %s", req.container_id);
            } else {
                res.status = -1;
                strcpy(res.message, "Clone failed");
            }
        } else if (req.kind == CMD_PS) {
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *it = ctx.containers;
            char list[256] = "ID\tPID\tSTATUS\n";
            while(it) {
                char line[64];
                snprintf(line, 64, "%s\t%d\t%d\n", it->id, it->host_pid, it->state);
                strncat(list, line, 255);
                it = it->next;
            }
            strncpy(res.message, list, 255);
            pthread_mutex_unlock(&ctx.metadata_lock);
        }

        write(cfd, &res, sizeof(res));
        close(cfd);
    }
    return 0;
}

/* --- Client Logic --- */
int main(int argc, char *argv[]) {
    if (argc < 2) { printf("Usage: %s <cmd>\n", argv[0]); return 1; }

    if (strcmp(argv[1], "supervisor") == 0) return run_supervisor(argv[2]);

    control_request_t req = {0};
    if (strcmp(argv[1], "run") == 0 && argc >= 5) {
        req.kind = CMD_RUN;
        strncpy(req.container_id, argv[2], CONTAINER_ID_LEN-1);
        strncpy(req.rootfs, argv[3], PATH_MAX-1);
        strncpy(req.command, argv[4], 255);
    } else if (strcmp(argv[1], "ps") == 0) {
        req.kind = CMD_PS;
    } else {
        return 1;
    }

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un addr = { .sun_family = AF_UNIX };
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path)-1);
    
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
        write(fd, &req, sizeof(req));
        control_response_t res;
        read(fd, &res, sizeof(res));
        printf("%s\n", res.message);
    } else {
        perror("Failed to connect to supervisor");
    }
    close(fd);
    return 0;
}
