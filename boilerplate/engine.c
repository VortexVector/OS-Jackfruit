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

/* --- Configuration --- */
#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 64

typedef enum { CMD_SUPERVISOR = 0, CMD_START, CMD_RUN, CMD_PS, CMD_LOGS, CMD_STOP } command_kind_t;
typedef enum { CONTAINER_RUNNING, CONTAINER_EXITED } container_state_t;

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
    int log_pipe_fd;
    struct container_record *next;
} container_record_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[256];
    long soft_limit_mib;
    long hard_limit_mib;
    int log_pipe_write_fd;
} control_request_t;

typedef struct {
    int status;
    char message[256];
} control_response_t;

typedef struct {
    int server_fd;
    bounded_buffer_t log_buffer;
    container_record_t *containers;
    pthread_mutex_t metadata_lock;
} supervisor_ctx_t;

supervisor_ctx_t ctx;

/* --- Bounded Buffer (Producer/Consumer) --- */
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

/* --- Logging & Child Execution --- */
void *pipe_reader(void *arg) {
    container_record_t *rec = arg;
    char buf[LOG_CHUNK_SIZE];
    while (1) {
        ssize_t n = read(rec->log_pipe_fd, buf, LOG_CHUNK_SIZE);
        if (n <= 0) break;
        log_item_t item;
        strncpy(item.container_id, rec->id, CONTAINER_ID_LEN);
        item.length = n;
        memcpy(item.data, buf, n);
        bb_push(&ctx.log_buffer, &item);
    }
    close(rec->log_pipe_fd);
    return NULL;
}
void *logger_thread(void *arg) {
    log_item_t item;
    while (bb_pop(&ctx.log_buffer, &item) == 0) {
        char path[PATH_MAX];
        snprintf(path, PATH_MAX, "%s/%s.log", LOG_DIR, item.container_id);
        int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0) { write(fd, item.data, item.length); close(fd); }
    }
    return NULL;
}

int child_fn(void *arg) {
    control_request_t *req = (control_request_t *)arg;
    sethostname(req->container_id, strlen(req->container_id));
    if (mount(req->rootfs, req->rootfs, NULL, MS_BIND, NULL) < 0) exit(1);
    if (chroot(req->rootfs) < 0 || chdir("/") < 0) exit(1);
    mount("proc", "/proc", "proc", 0, NULL);
    dup2(req->log_pipe_write_fd, 1);
    dup2(req->log_pipe_write_fd, 2);
    close(req->log_pipe_write_fd);
    char *argv[] = {"/bin/sh", "-c", req->command, NULL};
    execvp(argv[0], argv);
    return 0;
}

/* --- Helper: launch a container and record it --- */
static pid_t launch_container(control_request_t *req) {
    int pipefd[2];
    pipe(pipefd);
    req->log_pipe_write_fd = pipefd[1];

    void *stack = malloc(STACK_SIZE);
    pid_t pid = clone(child_fn, stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWUTS | SIGCHLD, req);
    if (pid < 0) return pid;

    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *rec = malloc(sizeof(container_record_t));
    strncpy(rec->id, req->container_id, CONTAINER_ID_LEN);
    rec->host_pid = pid;
    rec->state = CONTAINER_RUNNING;
    rec->log_pipe_fd = pipefd[0];
    rec->next = ctx.containers;
    ctx.containers = rec;

    // Start pipe reader thread
    pthread_t tid;
    pthread_create(&tid, NULL, pipe_reader, rec);
    pthread_detach(tid);

    pthread_mutex_unlock(&ctx.metadata_lock);

    // Register with monitor if limits set
    if (req->soft_limit_mib > 0 || req->hard_limit_mib > 0) {
        int fd = open("/dev/container_monitor", O_RDWR);
        if (fd >= 0) {
            struct monitor_request mon_req;
            mon_req.pid = pid;
            strncpy(mon_req.container_id, req->container_id, MONITOR_NAME_LEN);
            mon_req.soft_limit_bytes = req->soft_limit_mib * 1024 * 1024;
            mon_req.hard_limit_bytes = req->hard_limit_mib * 1024 * 1024;
            ioctl(fd, MONITOR_REGISTER, &mon_req);
            close(fd);
        }
    }

    return pid;
}

/* --- Supervisor Event Loop --- */
int run_supervisor(const char *rootfs) {
    mkdir(LOG_DIR, 0755);
    bb_init(&ctx.log_buffer);
    pthread_mutex_init(&ctx.metadata_lock, NULL);
    pthread_t tid;
    pthread_create(&tid, NULL, logger_thread, NULL);

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un addr = { .sun_family = AF_UNIX };
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path)-1);
    unlink(CONTROL_PATH);
    bind(ctx.server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(ctx.server_fd, 10);

    printf("[Supervisor] Active on: %s\n", CONTROL_PATH);
    while (1) {
        int cfd = accept(ctx.server_fd, NULL, NULL);
        control_request_t req;
        read(cfd, &req, sizeof(req));
        control_response_t res = {0, "Unknown command"};

        if (req.kind == CMD_RUN || req.kind == CMD_START) {
            /* CMD_RUN and CMD_START both launch a container */
            pid_t pid = launch_container(&req);
            if (pid < 0)
                snprintf(res.message, 255, "Failed to launch: %s (errno: %d)", req.container_id, errno);
            else
                snprintf(res.message, 255, "Started ID: %s (PID: %d)", req.container_id, pid);

        } else if (req.kind == CMD_PS) {
            /* List all known containers and their states */
            pthread_mutex_lock(&ctx.metadata_lock);
            char buf[256] = "";
            int n = 0;
            container_record_t *c = ctx.containers;
            while (c) {
                /* Reap if exited */
                int wstatus;
                pid_t r = waitpid(c->host_pid, &wstatus, WNOHANG);
                if (r > 0) c->state = CONTAINER_EXITED;

                char entry[64];
                snprintf(entry, sizeof(entry), "%s(%s) ",
                         c->id,
                         c->state == CONTAINER_RUNNING ? "running" : "exited");
                strncat(buf, entry, sizeof(buf) - strlen(buf) - 1);
                n++;
                c = c->next;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
            if (n == 0)
                snprintf(res.message, 255, "No containers");
            else
                snprintf(res.message, 255, "%s", buf);

        } else if (req.kind == CMD_LOGS) {
            /* Read log file for the requested container */
            char path[PATH_MAX];
            snprintf(path, PATH_MAX, "%s/%s.log", LOG_DIR, req.container_id);
            int lfd = open(path, O_RDONLY);
            if (lfd < 0) {
                snprintf(res.message, 255, "No logs found for: %s", req.container_id);
            } else {
                ssize_t n = read(lfd, res.message, sizeof(res.message) - 1);
                if (n <= 0)
                    snprintf(res.message, 255, "Log empty for: %s", req.container_id);
                else
                    res.message[n] = '\0';
                close(lfd);
            }

        } else if (req.kind == CMD_STOP) {
            /* Find, kill and remove the container record */
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *c = ctx.containers, *prev = NULL;
            while (c && strncmp(c->id, req.container_id, CONTAINER_ID_LEN) != 0) {
                prev = c;
                c = c->next;
            }
            if (c) {
                kill(c->host_pid, SIGKILL);
                waitpid(c->host_pid, NULL, 0);

                // Unregister from monitor
                int fd = open("/dev/container_monitor", O_RDWR);
                if (fd >= 0) {
                    struct monitor_request mon_req;
                    mon_req.pid = c->host_pid;
                    strncpy(mon_req.container_id, c->id, MONITOR_NAME_LEN);
                    ioctl(fd, MONITOR_UNREGISTER, &mon_req);
                    close(fd);
                }

                if (prev) prev->next = c->next;
                else ctx.containers = c->next;
                free(c);
                snprintf(res.message, 255, "Stopped: %s", req.container_id);
            } else {
                snprintf(res.message, 255, "Not found: %s", req.container_id);
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
        }

        write(cfd, &res, sizeof(res));
        close(cfd);
    }
    return 0;
}

/* --- Main / Client --- */
int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <rootfs>\n"
            "  %s start <name> <rootfs> <cmd> [--soft-mib N] [--hard-mib N]\n"
            "  %s run   <name> <rootfs> <cmd>\n"
            "  %s list\n"
            "  %s logs  <name>\n"
            "  %s stop  <name>\n",
            argv[0], argv[0], argv[0], argv[0], argv[0], argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0)
        return run_supervisor(argc >= 3 ? argv[2] : NULL);

    control_request_t req = {0};

    if ((strcmp(argv[1], "run") == 0 || strcmp(argv[1], "start") == 0) && argc >= 5) {
        req.kind = (strcmp(argv[1], "start") == 0) ? CMD_START : CMD_RUN;
        strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
        strncpy(req.rootfs,       argv[3], PATH_MAX - 1);
        strncpy(req.command,      argv[4], 255);

        /* Parse optional --soft-mib / --hard-mib flags */
        for (int i = 5; i < argc - 1; i++) {
            if (strcmp(argv[i], "--soft-mib") == 0)
                req.soft_limit_mib = atol(argv[++i]);
            else if (strcmp(argv[i], "--hard-mib") == 0)
                req.hard_limit_mib = atol(argv[++i]);
        }

    } else if (strcmp(argv[1], "list") == 0 || strcmp(argv[1], "ps") == 0) {
        req.kind = CMD_PS;

    } else if (strcmp(argv[1], "logs") == 0 && argc >= 3) {
        req.kind = CMD_LOGS;
        strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);

    } else if (strcmp(argv[1], "stop") == 0 && argc >= 3) {
        req.kind = CMD_STOP;
        strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);

    } else {
        fprintf(stderr, "Unknown command or missing arguments: %s\n", argv[1]);
        return 1;
    }

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un addr = { .sun_family = AF_UNIX };
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Cannot connect to supervisor at %s\n", CONTROL_PATH);
        close(fd);
        return 1;
    }
    write(fd, &req, sizeof(req));
    control_response_t res;
    read(fd, &res, sizeof(res));
    printf("Supervisor Response: %s\n", res.message);
    close(fd);
    return 0;
}
