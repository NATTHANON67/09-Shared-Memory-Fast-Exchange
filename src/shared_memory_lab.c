
#define _DEFAULT_SOURCE

#include "shared_memory_lab.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <unistd.h>

int create_segment(void) {
    return shmget(IPC_PRIVATE, sizeof(struct shared_block),
                  0600 | IPC_CREAT);
}

void cleanup_segment(int segment_id, struct shared_block *block) {
    if (shmdt(block) == -1) {
        perror("shmdt");
    }

    if (shmctl(segment_id, IPC_RMID, NULL) == -1) {
        perror("shmctl");
    }
}

/* Wait until shared memory reaches a particular state. */
static void wait_for_state(const struct shared_block *block, int state) {
    const volatile int *ready = &block->ready;

    while (*ready != state) {
        usleep(1000);
    }
}

static void wait_for_ready(const struct shared_block *block) {
    wait_for_state(block, 1);
}


/* TASK A: Fast exchange using shared memory. */
int task_fast_exchange(void) {
    int segment_id;
    pid_t child_pid;
    int status;
    struct shared_block *block;

    segment_id = create_segment();

    if (segment_id == -1) {
        perror("shmget");
        return 1;
    }

    block = shmat(segment_id, NULL, 0);

    if (block == (void *)-1) {
        perror("shmat");
        shmctl(segment_id, IPC_RMID, NULL);
        return 1;
    }

    memset(block, 0, sizeof(*block));

    child_pid = fork();

    if (child_pid < 0) {
        perror("fork");
        cleanup_segment(segment_id, block);
        return 1;
    }

    if (child_pid == 0) {
        char received[32];

        /* Receive the first part. */
        wait_for_state(block, 1);

        snprintf(received, sizeof(received), "%s",
                 block->message);

        /* Tell parent the first part has been read. */
        block->ready = 2;

        /* Receive the second part. */
        wait_for_state(block, 3);

        strncat(received, block->message,
                sizeof(received) - strlen(received) - 1);

        printf("child fast read: %s\n", received);
        fflush(stdout);

        if (shmdt(block) == -1) {
            perror("shmdt");
            _exit(1);
        }

        _exit(0);
    }

    /* First part of the message. */
    snprintf(block->message, sizeof(block->message),
             "%s", "shared memory ");

    block->ready = 1;

    /* Wait until child has copied the first part. */
    wait_for_state(block, 2);

    /* Second part of the message. */
    snprintf(block->message, sizeof(block->message),
             "%s", "is fast");

    block->ready = 3;

    if (waitpid(child_pid, &status, 0) == -1) {
        perror("waitpid");
        cleanup_segment(segment_id, block);
        return 1;
    }

    cleanup_segment(segment_id, block);

    return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : 1;
}


/* TASK B: Shared memory only keeps the latest value. */
int task_latest_value_limit(void) {
    int segment_id;
    pid_t child_pid;
    int status;
    struct shared_block *block;

    segment_id = create_segment();

    if (segment_id == -1) {
        perror("shmget");
        return 1;
    }

    block = shmat(segment_id, NULL, 0);

    if (block == (void *)-1) {
        perror("shmat");
        shmctl(segment_id, IPC_RMID, NULL);
        return 1;
    }

    memset(block, 0, sizeof(*block));

    child_pid = fork();

    if (child_pid < 0) {
        perror("fork");
        cleanup_segment(segment_id, block);
        return 1;
    }

    if (child_pid == 0) {
        wait_for_ready(block);

        printf("child latest view: %s\n", block->message);
        fflush(stdout);

        if (shmdt(block) == -1) {
            perror("shmdt");
            _exit(1);
        }

        _exit(0);
    }

    /* First update. */
    snprintf(block->message, sizeof(block->message),
             "%s", "first update");

    /* Overwrite it with the latest update. */
    snprintf(block->message, sizeof(block->message),
             "%s", "latest-only");

    block->ready = 1;

    if (waitpid(child_pid, &status, 0) == -1) {
        perror("waitpid");
        cleanup_segment(segment_id, block);
        return 1;
    }

    cleanup_segment(segment_id, block);

    return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : 1;
}


/* TASK C: Demonstrate fixed buffer capacity. */
int task_capacity_limit(void) {
    int segment_id;
    pid_t child_pid;
    int status;
    struct shared_block *block;

    const char *source = "abcdefghijklmnopqrstuvwxyz";

    segment_id = create_segment();

    if (segment_id == -1) {
        perror("shmget");
        return 1;
    }

    block = shmat(segment_id, NULL, 0);

    if (block == (void *)-1) {
        perror("shmat");
        shmctl(segment_id, IPC_RMID, NULL);
        return 1;
    }

    memset(block, 0, sizeof(*block));

    child_pid = fork();

    if (child_pid < 0) {
        perror("fork");
        cleanup_segment(segment_id, block);
        return 1;
    }

    if (child_pid == 0) {
        wait_for_ready(block);

        printf("child capacity bytes: %zu\n",
               strlen(block->message));
        fflush(stdout);

        if (shmdt(block) == -1) {
            perror("shmdt");
            _exit(1);
        }

        _exit(0);
    }

    /* Copy source safely into the bounded buffer. */
    snprintf(block->message, sizeof(block->message),
             "%s", source);

    block->ready = 1;

    if (waitpid(child_pid, &status, 0) == -1) {
        perror("waitpid");
        cleanup_segment(segment_id, block);
        return 1;
    }

    printf("parent source bytes: %zu\n", strlen(source));

    cleanup_segment(segment_id, block);

    return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : 1;
}


/* MAIN */
int main(void) {
    if (task_fast_exchange() != 0) {
        return 1;
    }

    if (task_latest_value_limit() != 0) {
        return 1;
    }

    if (task_capacity_limit() != 0) {
        return 1;
    }

    printf("all tasks done\n");

    return 0;
}
