/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <stdio.h> //tmp

#define BARRIER_TAG -1
#define BRODCAST_TAG -2
#define REDUCE_TAG -3
#define ATOMIC_BLOCK_SIZE 512
#define METADATA_SIZE ((int)(5 * sizeof(int)))
#define DATA_IN_METADATA (ATOMIC_BLOCK_SIZE - METADATA_SIZE)
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

struct metadata {
    int sender, leftsize, size, tag, should_break;
    int8_t data[DATA_IN_METADATA];
};
struct message {
    int sender, size, tag, should_break;
    void *data;
};
struct message_list {
    struct message *msg;
    struct message_list *prev, *next; 
};

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();
    if (enable_deadlock_detection)
        TODO
}

void MIMPI_Finalize() {
    int myrank = MIMPI_World_rank(), n = MIMPI_World_size();
    for (int i = 0; i < n; i++) {
        if (i != myrank) {
            ASSERT_SYS_OK(close(READ_DESC(i, myrank, n)));
            ASSERT_SYS_OK(close(WRITE_DESC(myrank, i, n)));
        }
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
    struct metadata meta = {.sender = myrank,
                          .leftsize = MAX(count - DATA_IN_METADATA, 0),
                          .size = abs(count),
                          .tag = tag,
                          .should_break = (count >= 0 ? 0 : 1)};
    if (count - meta.leftsize > 0)
        memcpy(meta.data, data, count - meta.leftsize);
    int res = chsend(WRITE_DESC(myrank, destination, n), &meta, sizeof(meta));
        if (res == -1)
            return MIMPI_ERROR_REMOTE_FINISHED;
    int left = meta.leftsize;
    while (left > 0) {
        res = chsend(WRITE_DESC(myrank, destination, n), data + count - left, left);
        if (res == -1)
            return MIMPI_ERROR_REMOTE_FINISHED;
        left -= res;
    }
    return MIMPI_SUCCESS;
}

int MIMPI_Correct_msg(struct message *msg, int source, int tag, int count) {
    return (msg->sender == source && msg->size == count &&
           (msg->tag == tag || tag == 0));
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
    
    // searching the list
    static struct message_list *msg_buffer_begin;
    static struct message_list *msg_buffer_end;
    if (msg_buffer_begin != NULL) {
        struct message_list *checked = msg_buffer_begin;
        while (checked != NULL) {
            if (MIMPI_Correct_msg(checked->msg, source, tag, count)) {
                int should_break = checked->msg->should_break;
                if (!should_break) // TODO: czy to ładne?
                    memcpy(data, checked->msg->data, checked->msg->size);
                if (checked == msg_buffer_begin)
                    msg_buffer_begin = checked->next;
                if (checked == msg_buffer_end)
                    msg_buffer_end = checked->prev;
                if (checked->next != NULL)
                    checked->next->prev = checked->prev;
                if (checked->prev != NULL)
                    checked->prev->next = checked->next;
                free(checked->msg->data);
                free(checked->msg);
                free(checked);
                if (should_break)
                    return MIMPI_ERROR_REMOTE_FINISHED;
                return MIMPI_SUCCESS;
            }
            checked = checked->next;
        }
    }

    // reading from pipe
    while (true) { // TMP
        struct metadata meta;
        int res = chrecv(READ_DESC(source, myrank, n), &meta, sizeof(meta));
        if (res <= 0) // EOF
            return MIMPI_ERROR_REMOTE_FINISHED;
        struct message *msg = (struct message*)malloc (sizeof(struct message));
        if (msg == NULL)
            syserr("No memory\n");
        msg->sender = meta.sender;
        msg->size = meta.size;
        msg->tag = meta.tag;
        msg->should_break = meta.should_break;
        msg->data = malloc(meta.size);
        if (msg->data == NULL)
            syserr("No memory\n");
        memcpy(msg->data, &meta.data, MIN(DATA_IN_METADATA, meta.size));
        int left = meta.leftsize;
        while (left > 0) {
            int res = chrecv(READ_DESC(source, myrank, n), msg->data + msg->size - left, left);
            if (res <= 0) // EOF
                return MIMPI_ERROR_REMOTE_FINISHED;
            left -= res;
        }
        if (MIMPI_Correct_msg(msg, source, tag, count)) {
            int should_break = msg->should_break;
            if (!should_break) // TODO: czy to ładne?
                memcpy(data, msg->data, meta.size);
            free(msg->data);
            free(msg);
            if (should_break)
                return MIMPI_ERROR_REMOTE_FINISHED;
            return MIMPI_SUCCESS;
        }
        else {
            struct message_list* el = (struct message_list*) malloc (sizeof(struct message_list));
            el->msg = msg;
            el->next = NULL;
            el->prev = msg_buffer_end;
            if (msg_buffer_end == NULL) {
                msg_buffer_begin = el;
                msg_buffer_end = el;
            }
            else {
                msg_buffer_end->next = el;
                msg_buffer_end = el;
            }
        }
    }
}

MIMPI_Retcode MIMPI_Barrier() {
    // built like a tree where process of rank v has children 2v + 1 and 2v + 2
    // and a parent numbered (v - 1)/2 -> therefore v = 0 is a root
    char msg = 0;
    return MIMPI_Bcast(&msg, 1, 0);
}

void reorganise(int children[2], int *parent, int root, int myrank) {
    if (root != 0) {
        if (children[0] == root)
            children[0] = 0;
        if (children[1] == root)
            children[1] = 0;
        if (*parent == root)
            *parent = 0;
        else if (*parent == 0)
            *parent = root;
        if (myrank == root) {
            children[0] = (root == 1 ? 0 : 1);
            children[1] = (root == 2 ? 0 : 2);
        }
        else if (myrank == 0) {
            children[0] = 2 * root + 1;
            children[1] = 2 * root + 2;
        }
    }
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    int n = MIMPI_World_size(), myrank = MIMPI_World_rank();
    int children[2] = {2 * myrank + 1, 2 * myrank + 2};
    int parent = (myrank - 1) / 2;
    reorganise(children, &parent, root, myrank);
    char msg = 's'; // s - success, b - barrier broken
    for (int i = 0; i < 2; i++) {
        if (children[i] < n) {
            if (MIMPI_Recv(&msg, 1, children[i], BARRIER_TAG) ==
                MIMPI_ERROR_REMOTE_FINISHED || msg == 'b') {
                    msg = 'b';
                    MIMPI_Send(&msg, 1, parent, BARRIER_TAG);
                    MIMPI_Send(data, -count, children[1 - i], BRODCAST_TAG);
                    return MIMPI_ERROR_REMOTE_FINISHED;
            }
        }
    }
    MIMPI_Send(&msg, 1, parent, BARRIER_TAG);
    if (myrank != root) {
        if (MIMPI_Recv(data, count, parent, BRODCAST_TAG) ==
            MIMPI_ERROR_REMOTE_FINISHED) {
                MIMPI_Send(data, -count, children[0], BRODCAST_TAG);
                MIMPI_Send(data, -count, children[1], BRODCAST_TAG);
                return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    MIMPI_Send(data, count, children[0], BRODCAST_TAG);
    MIMPI_Send(data, count, children[1], BRODCAST_TAG);
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    int n = MIMPI_World_size(), myrank = MIMPI_World_rank();
    int children[2] = {2 * myrank + 1, 2 * myrank + 2};
    int parent = (myrank - 1) / 2;
    reorganise(children, &parent, root, myrank);
    char msg = 's'; // s - success, b - barrier broken
    int8_t ans[count], rcv[count];
    memcpy(ans, send_data, count);
    for (int i = 0; i < 2; i++) {
        if (children[i] < n) {
            if (MIMPI_Recv(rcv, count, children[i], REDUCE_TAG) ==
                MIMPI_ERROR_REMOTE_FINISHED) {
                    msg = 'b';
                    MIMPI_Send(ans, -count, parent, REDUCE_TAG);
                    MIMPI_Send(&msg, 1, children[1 - i], BARRIER_TAG);
                    return MIMPI_ERROR_REMOTE_FINISHED;
            }
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
    if (myrank != root)
        MIMPI_Send(ans, count, parent, REDUCE_TAG);
    else
        memcpy(recv_data, ans, count);
    if (myrank != root) {
        if (MIMPI_Recv(&msg, 1, parent, BARRIER_TAG) ==
            MIMPI_ERROR_REMOTE_FINISHED || msg == 'b') {
                msg = 'b';
                MIMPI_Send(&msg, 1, children[0], BARRIER_TAG);
                MIMPI_Send(&msg, 1, children[1], BARRIER_TAG);
                return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    MIMPI_Send(&msg, 1, children[0], BARRIER_TAG);
    MIMPI_Send(&msg, 1, children[1], BARRIER_TAG);    
    return MIMPI_SUCCESS;
}