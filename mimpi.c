/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <stdio.h> //tmp

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();
    if (enable_deadlock_detection)
        TODO

    // TODO
}

void MIMPI_Finalize() {
    //TODO
    int myrank = MIMPI_World_rank(), n = MIMPI_World_size();
    for (int i = 0; i < n; i++) {
        if (i == myrank)
            continue;
        ASSERT_SYS_OK(close(READ_DESC(i, myrank, n)));
        ASSERT_SYS_OK(close(WRITE_DESC(myrank, i, n)));
    }

    channels_finalize();
}

int MIMPI_World_size() {
    return atoi(getenv(WORLDSIZE_NAME));
}

int MIMPI_World_rank() {
    return atoi(getenv(RANK_NAME));
}

MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag
) {
    int n = MIMPI_World_size();
    if (destination >= n || destination < 0)
        return MIMPI_ERROR_NO_SUCH_RANK;
    int myrank = MIMPI_World_rank();
    if (destination == myrank)
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    int left = count;
    while (left > 0) {
        int res = chsend(WRITE_DESC(myrank, destination, n), data + count - left, left);
        if (res == -1)
            return MIMPI_ERROR_REMOTE_FINISHED;
        left -= res;
    }
    return MIMPI_SUCCESS;
    // TODO: usprawnienia
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    int n = MIMPI_World_size();
    if (source >= n || source < 0)
        return MIMPI_ERROR_NO_SUCH_RANK;
    int myrank = MIMPI_World_rank();
    if (source == myrank)
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    int left = count;
    while (left > 0) {
        int res = chrecv(READ_DESC(source, myrank, n), data + count - left, left);
        if (res <= 0) // EOF
            return MIMPI_ERROR_REMOTE_FINISHED;
        left -= res;
    }
    return MIMPI_SUCCESS;
    // TODO: usprawnienia
}

#define BARRIER_TAG -1
#define BRODCAST_TAG -2
#define REDUCE_TAG -3

MIMPI_Retcode MIMPI_Barrier() {
    int n = MIMPI_World_size(), myrank = MIMPI_World_rank();
    char msg = 42; // TODO: wywalić - będzie działać jak wysylamy metadane
    for (int i = 0; i < n; i++) {
        if (i == myrank) continue;
        if (MIMPI_Send(&msg, 1, i, BARRIER_TAG) == MIMPI_ERROR_REMOTE_FINISHED)
            return MIMPI_ERROR_REMOTE_FINISHED;
    }
    for (int i = 0; i < n; i++) {
        if (i == myrank) continue;
        if (MIMPI_Recv(&msg, 1, i, BARRIER_TAG) == MIMPI_ERROR_REMOTE_FINISHED)
            return MIMPI_ERROR_REMOTE_FINISHED;
        //printf("%d\n", msg);
    }
    return MIMPI_SUCCESS;
    // TODO: drzewo
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    int n = MIMPI_World_size(), myrank = MIMPI_World_rank();
    MIMPI_Retcode res = MIMPI_Barrier();
    if (res != MIMPI_SUCCESS)
        return res;
    if (myrank == root) {
        for (int i = 0; i < n; i++) {
            if (i != myrank) {
                res = MIMPI_Send(data, count, i, BRODCAST_TAG);
                if (res != MIMPI_SUCCESS)
                    return res;
            }
        }
    }
    res = MIMPI_Barrier();
    if (res != MIMPI_SUCCESS)
        return res;
    if (myrank != root) {
        res = MIMPI_Recv(data, count, root, BRODCAST_TAG);
        if (res != MIMPI_SUCCESS)
            return res;
    }
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) { // TODO: drzewo
    int n = MIMPI_World_size(), myrank = MIMPI_World_rank();
    MIMPI_Retcode res = MIMPI_Barrier();
    if (res != MIMPI_SUCCESS)
        return res;
    if (myrank != root) {
        res = MIMPI_Send(send_data, count, root, BRODCAST_TAG);
        if (res != MIMPI_SUCCESS)
            return res;
    }
    else {
        int8_t* ans = recv_data;
        int8_t rcv[count];
        for (int i = 0; i < count; i++)
            ans[i] = ((int8_t*) send_data)[i];
        for (int i = 0; i < n; i++) {
            if (i != myrank) {
                res = MIMPI_Recv(rcv, count, i, BRODCAST_TAG);
                if (res != MIMPI_SUCCESS)
                    return res;
                switch (op) {
                case MIMPI_MAX:
                    for (int j = 0; j < count; j++)
                        if (rcv[j] > ans[j])
                            ans[j] = rcv[j];
                    break;
                case MIMPI_MIN:
                    for (int j = 0; j < count; j++)
                        if (rcv[j] < ans[j])
                            ans[j] = rcv[j];
                    break;
                case MIMPI_SUM:
                    for (int j = 0; j < count; j++)
                        ans[j] += rcv[j];
                    break;   
                case MIMPI_PROD:
                    for (int j = 0; j < count; j++)
                        ans[j] *= rcv[j];
                    break;
                }
            }
        }
    }
    /*res = MIMPI_Barrier();
    if (res != MIMPI_SUCCESS)
        return res;*/
    return MIMPI_SUCCESS; 
}