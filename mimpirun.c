/**
 * This file is for implementation of mimpirun program.
 * */

#include "mimpi_common.h"
#include "channel.h"
#include <pthread.h>
#include <unistd.h>
#include <sys/wait.h>
#include <stdio.h> // TMP

int main(int argc, char *argv[]) {
    // 1. prepare
    assert(argc >= 3); // TMP
    int n_instances = atoi(argv[1]);
    ASSERT_SYS_OK(setenv(WORLDSIZE_NAME, argv[1], 1));

    int desc[2];
    int pipe_r[n_instances][n_instances], pipe_w[n_instances][n_instances];
    int next = 20;
    for (int i = 0; i < n_instances; i++) {
        for (int j = 0; j < n_instances; j++) {
            // TODO: skip for i = j
            channel(desc);
            ASSERT_SYS_OK(dup2(desc[1], next + 1));
            ASSERT_SYS_OK(close(desc[1]));
            ASSERT_SYS_OK(dup2(desc[0], next));
            ASSERT_SYS_OK(close(desc[0]));
            pipe_r[i][j] = next; // reading messages from i to j
            pipe_w[i][j] = next + 1; // writing messages from i to j
            next += 2;
        }
    }
    

    // 2. start all
    for (int i = 0; i < n_instances; i++) {
        pid_t pid = fork();
        ASSERT_SYS_OK(pid);
        if (!pid) {
            for (int j = 0; j < n_instances; j++) {
                for (int k = 0; k < n_instances; k++) {
                    if (j == i && k != i) {
                        if (pipe_r[j][k] != -1) {
                            ASSERT_SYS_OK(close(pipe_r[j][k]));
                            pipe_r[j][k] = -1;
                        }
                    }
                    else if (k == i && j != i) {
                        if (pipe_w[j][k] != -1) {
                            ASSERT_SYS_OK(close(pipe_w[j][k]));
                            pipe_w[j][k] = -1;
                        }
                    }
                    else {
                        if (pipe_w[j][k] != -1) {
                            ASSERT_SYS_OK(close(pipe_w[j][k]));
                            pipe_w[j][k] = -1;
                        }
                        if (pipe_r[j][k] != -1) {
                            ASSERT_SYS_OK(close(pipe_r[j][k]));
                            pipe_r[j][k] = -1;
                        }
                    }
                }
            }
            char rank_temp[3];
            ASSERT_SYS_OK(sprintf(rank_temp, "%2hi", i));
            ASSERT_SYS_OK(setenv(RANK_NAME, rank_temp, 1));
            ASSERT_SYS_OK(execvp(argv[2], argv + 2));
        }
        else {
            for (int j = 0; j < n_instances; j++) {
                for (int k = 0; k < n_instances; k++) {
                    if (j == i && k != i && pipe_w[j][k] != -1) {
                        ASSERT_SYS_OK(close(pipe_w[j][k]));
                        pipe_w[j][k] = -1;
                    }
                    else if (k == i && j != i && pipe_r[j][k] != -1) {
                        ASSERT_SYS_OK(close(pipe_r[j][k]));
                        pipe_r[j][k] = -1;
                    }
                    else if (k == i && j == i) {
                        ASSERT_SYS_OK(close(pipe_r[j][k]));
                        pipe_r[j][k] = -1;
                        ASSERT_SYS_OK(close(pipe_w[j][k]));
                        pipe_w[j][k] = -1;
                    }
                }
            }
        }
        //usleep(10000); // TMP
    }


    // 3. wait for children ending
    for (int i = 0; i < n_instances; i++)
        ASSERT_SYS_OK(wait(NULL));


    // 4. finish
    
}