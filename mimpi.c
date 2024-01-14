/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <pthread.h>

#define BRODCAST_TAG -1
#define REDUCE_TAG -2
#define ATOMIC_BLOCK_SIZE 512
#define METADATA_SIZE ((int)(5 * sizeof(int)))
#define DATA_IN_METADATA (ATOMIC_BLOCK_SIZE - METADATA_SIZE)
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define MUTEX_NAME "MIMPI_mutex"
#define THREADS_NAME "MIMPI_threads"
#define LISTS_NAME "MIMPI_lists"
#define CONDS_NAME "MIMPI_conditionals"
#define PTR_LEN(T) (4 * sizeof(T*)) // overapproximated decimal length of ptr to T

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
struct message_list_header {
    struct message_list *first, *last;
    int read_finished;
};
struct sent_to_thread {
    int source;
    pthread_mutex_t *mutex;
    struct message_list_header *list;
    pthread_cond_t *cond;
};

void *MIMPI_Read (void *source);

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();
    if (enable_deadlock_detection)
        TODO
    int myrank = MIMPI_World_rank(), n = MIMPI_World_size();
    pthread_t *threads = malloc (n * sizeof(pthread_t));
    pthread_mutex_t *mutexes = malloc (n * sizeof(pthread_mutex_t));
    struct message_list_header *lists = malloc (n * sizeof(struct message_list_header));
    pthread_cond_t *conds = malloc (n * sizeof(pthread_cond_t));
    if (threads == NULL || mutexes == NULL || lists == NULL || conds == NULL)
        syserr("malloc failed");
    for (int i = 0; i < n; i++)
        ASSERT_ZERO(pthread_mutex_init(mutexes + i, NULL));
    for (int i = 0; i < n; i++) {
        lists[i].first = NULL;
        lists[i].last = NULL;
        lists[i].read_finished = 0;
    }
    for (int i = 0; i < n; i++)
        ASSERT_ZERO(pthread_cond_init(conds + i, NULL));

    char mutex_str[PTR_LEN(pthread_mutex_t)];
    ASSERT_SYS_OK(snprintf(mutex_str, PTR_LEN(pthread_mutex_t), "%p", mutexes));
    ASSERT_SYS_OK(setenv(MUTEX_NAME, mutex_str, 1));
    char threads_str[PTR_LEN(pthread_t)];
    ASSERT_SYS_OK(snprintf(threads_str, PTR_LEN(pthread_t), "%p", threads));
    ASSERT_SYS_OK(setenv(THREADS_NAME, threads_str, 1));
    char lists_str[PTR_LEN(struct message_list_header)];
    ASSERT_SYS_OK(snprintf(lists_str, PTR_LEN(struct message_list_header), "%p", lists));
    ASSERT_SYS_OK(setenv(LISTS_NAME, lists_str, 1));
    char conds_str[PTR_LEN(pthread_cond_t)];
    ASSERT_SYS_OK(snprintf(conds_str, PTR_LEN(pthread_cond_t), "%p", conds));
    ASSERT_SYS_OK(setenv(CONDS_NAME, conds_str, 1));


    for (int i = 0; i < n; i++) {
        if (i != myrank) {
            struct sent_to_thread* arg = malloc(sizeof(struct sent_to_thread));
            if (arg == NULL)
                syserr("malloc failed");
            arg->source = i;
            arg->mutex = mutexes + i;
            arg->list = lists + i;
            arg->cond = conds + i;
            ASSERT_ZERO(pthread_create(&threads[i], NULL, MIMPI_Read, (void*)arg));
        }
    }
}

void MIMPI_Finalize() {
    int myrank = MIMPI_World_rank(), n = MIMPI_World_size();
    for (int i = 0; i < n; i++) {
        if (i != myrank) {
            ASSERT_SYS_OK(close(READ_DESC(i, myrank, n)));
            ASSERT_SYS_OK(close(WRITE_DESC(myrank, i, n)));
        }
    }
    
    pthread_t *threads;
    sscanf(getenv(THREADS_NAME), "%p", &threads);
    for (int i = 0; i < n; i++) {
        if (i != myrank) {
            ASSERT_ZERO(pthread_join(threads[i], NULL));
        }
    }
    free(threads);
    ASSERT_SYS_OK(unsetenv(THREADS_NAME));
    pthread_mutex_t *mutexes;
    sscanf(getenv(MUTEX_NAME), "%p", &mutexes);
    free(mutexes); // TODO: destroying particular mutexes
    ASSERT_SYS_OK(unsetenv(MUTEX_NAME));
    struct message_list_header *lists;
    sscanf(getenv(LISTS_NAME), "%p", &lists);
    free(lists);
    ASSERT_SYS_OK(unsetenv(LISTS_NAME));
    pthread_cond_t *conds;
    sscanf(getenv(CONDS_NAME), "%p", &conds);
    //for (int i = 0; i < n; i++) // TODO: przywrócić to tu
    //    ASSERT_ZERO(pthread_cond_destroy(conds + i));
    free(conds);
    ASSERT_SYS_OK(unsetenv(CONDS_NAME));
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
    memset(meta.data, 0, DATA_IN_METADATA);
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
           (msg->tag == tag || tag == MIMPI_ANY_TAG));
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

    pthread_mutex_t *mutexes; // TODO: create a tiny function for this
    sscanf(getenv(MUTEX_NAME), "%p", &mutexes);
    struct message_list_header *lists; // and this
    sscanf(getenv(LISTS_NAME), "%p", &lists);
    pthread_cond_t *conds; // and this
    sscanf(getenv(CONDS_NAME), "%p", &conds);

    pthread_mutex_t *mutex = mutexes + source;
    struct message_list_header *list = lists + source;
    pthread_cond_t *cond = conds + source;
    
    ASSERT_ZERO(pthread_mutex_lock(mutex));
    struct message_list *checked = list->first;
    struct message_list *prev_checked = checked;
    // searching the list
    while (true) {
        while (checked != NULL) {
            if (MIMPI_Correct_msg(checked->msg, source, tag, count)) {
                int should_break = checked->msg->should_break;
                if (!should_break) // TODO: czy to ładne?
                    memcpy(data, checked->msg->data, checked->msg->size);
                if (checked == list->first)
                    list->first = checked->next;
                if (checked == list->last)
                    list->last = checked->prev;
                if (checked->next != NULL)
                    checked->next->prev = checked->prev;
                if (checked->prev != NULL)
                    checked->prev->next = checked->next;
                free(checked->msg->data);
                free(checked->msg);
                free(checked);
                ASSERT_ZERO(pthread_mutex_unlock(mutex));
                if (should_break)
                    return MIMPI_ERROR_REMOTE_FINISHED;
                return MIMPI_SUCCESS;
            }
            prev_checked = checked;
            checked = checked->next;
        }
        checked = prev_checked;
        while (((checked == NULL && list->first == NULL) ||
                (checked != NULL && checked->next == NULL)) &&
                !list->read_finished) {
            ASSERT_ZERO(pthread_cond_wait(cond, mutex));
        }
        if (checked == NULL)
            checked = list->first;
        if (checked == NULL && list->read_finished) {
            ASSERT_ZERO(pthread_mutex_unlock(mutex));
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
}

MIMPI_Retcode MIMPI_Barrier() {
    // built like a tree where process of rank v has children 2v + 1 and 2v + 2
    // and a parent numbered (v - 1)/2 -> therefore v = 0 is a root
    int8_t msg = 0;
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
            if (root > 2)
                *parent = (root - 1) / 2;
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
            if (MIMPI_Recv(&msg, 1, children[i], BRODCAST_TAG) ==
                MIMPI_ERROR_REMOTE_FINISHED || msg == 'b') {
                    msg = 'b';
                    MIMPI_Send(&msg, 1, parent, BRODCAST_TAG);
                    MIMPI_Send(data, -count, children[1 - i], BRODCAST_TAG);
                    return MIMPI_ERROR_REMOTE_FINISHED;
            }
        }
    }
    MIMPI_Send(&msg, 1, parent, BRODCAST_TAG);
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
                    MIMPI_Send(&msg, 1, children[1 - i], REDUCE_TAG);
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
        if (MIMPI_Recv(&msg, 1, parent, REDUCE_TAG) ==
            MIMPI_ERROR_REMOTE_FINISHED || msg == 'b') {
                msg = 'b';
                MIMPI_Send(&msg, 1, children[0], REDUCE_TAG);
                MIMPI_Send(&msg, 1, children[1], REDUCE_TAG);
                return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    MIMPI_Send(&msg, 1, children[0], REDUCE_TAG);
    MIMPI_Send(&msg, 1, children[1], REDUCE_TAG);    
    return MIMPI_SUCCESS;
}


void *MIMPI_Read (void *arg) {
    int myrank = MIMPI_World_rank(), n = MIMPI_World_size();
    struct sent_to_thread *sent = (struct sent_to_thread*) arg;

// reading from pipe
    struct metadata meta;
    while (true) {
        int res = chrecv(READ_DESC(sent->source, myrank, n), &meta, sizeof(meta));
        if (res <= 0) { // EOF
            ASSERT_ZERO(pthread_mutex_lock(sent->mutex));
            sent->list->read_finished = 1;
            ASSERT_ZERO(pthread_cond_signal(sent->cond));
            ASSERT_ZERO(pthread_mutex_unlock(sent->mutex));
            free(arg);
            return NULL;
        }
        struct message *msg = (struct message*)malloc (sizeof(struct message));
        if (msg == NULL)
            syserr("malloc failed");
        msg->sender = meta.sender;
        msg->size = meta.size;
        msg->tag = meta.tag;
        msg->should_break = meta.should_break;
        msg->data = malloc(meta.size);
        if (msg->data == NULL)
            syserr("malloc failed");
        memcpy(msg->data, &meta.data, MIN(DATA_IN_METADATA, meta.size));
        int left = meta.leftsize;
        while (left > 0) {
            int res = chrecv(READ_DESC(sent->source, myrank, n), msg->data + msg->size - left, left);
            if (res <= 0) { // EOF
                ASSERT_ZERO(pthread_mutex_lock(sent->mutex));
                sent->list->read_finished = 1;
                ASSERT_ZERO(pthread_cond_signal(sent->cond));
                ASSERT_ZERO(pthread_mutex_unlock(sent->mutex));
                free(arg);
                return NULL;
            }
            left -= res;
        }
        struct message_list* el = malloc (sizeof(struct message_list));
        if (el == NULL)
            syserr("malloc failed");
        ASSERT_ZERO(pthread_mutex_lock(sent->mutex));
        el->msg = msg;
        el->next = NULL;
        el->prev = sent->list->last;
        if (sent->list->last == NULL) {
            sent->list->last = el;
            sent->list->first = el;
        }
        else {
            sent->list->last->next = el;
            sent->list->last = el;
        }
        ASSERT_ZERO(pthread_cond_signal(sent->cond));
        ASSERT_ZERO(pthread_mutex_unlock(sent->mutex));
    }
    free(arg);
    return NULL;
}