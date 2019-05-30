/******************************************************************************
 * Your implementation of the MapReduce framework API.
 *
 * Other than this comment, you are free to modify this file as much as you
 * want, as long as you implement the API specified in the header file.
 *
 * Note: where the specification talks about the "caller", this is the program
 * which is not your code.  If the caller is required to do something, that
 * means your code may assume it has been done.
 ******************************************************************************/
#include <stdlib.h>
#include <stdio.h>
#include "mapreduce.h"
#include <pthread.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
extern int errno;

/********************** Helper functions ****************************************/
 
//Wrapper function for Map
//this function will be passed into pthread_create to invoke mapper
static void *Map_fn(void *args) {
    struct map_param *mp = args;
    int (*Map)(struct map_reduce*, int, int, int) = mp->mr->map_fn;
    int *ret; //Hold return value 
    if ((ret = (int *)malloc(sizeof(int))) == NULL)
        printf("Mapper return value not created\n ");
    
    *ret = (*Map)(mp->mr, mp->infd, mp->mapID, mp->nmaps);
    mp->mr->join[mp->mapID] = 1; //Update thread status 
    //Signal consume function
    if (pthread_cond_broadcast(&(mp->mr->fill[mp->mapID])) != 0) {
        printf("Signal error in mapper\n");
        exit(2);
    }
    pthread_exit((void*)ret);
}

//Wrapper function for Reduce
//This function will be passed into pthread_create to invoke reducer
static void *Reduce_fn(void *args) {
    struct reduce_param *rp = args;
    int (*Reduce)(struct map_reduce*, int, int) = rp->mr->reduce_fn;
    int *ret;
    if ((ret = (int *)malloc(sizeof(int))) == NULL)
        printf("Reducer return value not created\n ");
    
    *ret  = (*Reduce)(rp->mr, rp->outfd, rp->nmaps);
    pthread_exit((void*)ret);
}

//Initialize queue
static void queue_init(int id, int size, struct queue_t *q) {
    struct node_t *tmp = (struct node_t*)malloc(sizeof(struct node_t));
    tmp->next = NULL;
    q->head = q->tail = tmp;
    q->size = size;   //Remaining size
    q->count = 0;     //Element count
    q->id = id;       //This buffer id
    pthread_mutex_init(&q->htLock, NULL);
}

//Enqueue
static void enqueue(struct queue_t *q,struct map_reduce *mr, const struct kvpair *kv) {
    struct node_t *tmp = (struct node_t*)malloc(sizeof(struct node_t));
    assert(tmp != NULL);
    int node_size = 2*sizeof(uint32_t)+kv->keysz+kv->valuesz;

    tmp->keysz = kv->keysz;
    tmp->valuesz = kv->valuesz;
    tmp->values = (void *)malloc(node_size);
    //Serialize data
    memcpy(tmp->values, kv->key, kv->keysz);
    memcpy((char *)tmp->values+kv->keysz, kv->value, kv->valuesz);
     
    tmp->nodesz = node_size;
    tmp->next  = NULL;
    pthread_mutex_lock(&q->htLock);
    q->size = q->size - node_size;
    q->count++;
    q->tail->next = tmp;
    q->tail = tmp;
    pthread_mutex_unlock(&q->htLock);
}

//Dequeue
static int dequeue(struct queue_t *q, const struct kvpair *kv) {
    pthread_mutex_lock(&q->htLock);
    struct node_t *tmp = q->head;
    struct node_t *newHead = tmp->next;
    if (newHead == NULL) {
        pthread_mutex_unlock(&q->htLock);
	return -1; // queue was empty
    }

    memcpy((void *)&kv->keysz, &newHead->keysz, sizeof(uint32_t));
    memcpy((void *)&kv->valuesz, &newHead->valuesz, sizeof(uint32_t));
    //Deserialize data
    memcpy(kv->key, newHead->values, kv->keysz);
    memcpy(kv->value, (char *)newHead->values+kv->keysz, kv->valuesz);
    
    q->size += newHead->nodesz; //Update remaining size in buffer 
    q->count--;         //Update size
    q->head = newHead;  
    pthread_mutex_unlock(&q->htLock);
    free(tmp->values);
    free(tmp);
    return 0;
}
/************************* End of Helper Functions *****************************/

struct map_reduce *mr_create(map_fn map, reduce_fn reduce, int threads, int buffer_size) {
    printf("Initializing map reduce\n");
    int i; //'for' loop iterator
    struct map_reduce *mr = (struct map_reduce*)malloc(sizeof(struct map_reduce));
    mr->map_fn = map;       //Map function pointer
    mr->reduce_fn = reduce; //Reduce function pointer
    mr->threads = threads;  //Number of threads 
    mr->buffer_size = buffer_size; //Buffer size 
    mr->join = (int *)malloc(threads*sizeof(int));  //Array to indicate thread status 
    mr->map_tid = (pthread_t*)malloc(mr->threads*sizeof(pthread_t)); //tid of mapper threads
    mr->reduce_tid = 0;     //tid of reducer threads
    mr->mfd = (int*)malloc(mr->threads*sizeof(int)); //mapper fds
    mr->rfd = 0;            //reducer fd

    // Create buffer for each thread
    struct queue_t arrayq[mr->threads];
    for (i = 0; i<mr->threads; i++){
       queue_init(i, mr->buffer_size, &arrayq[i]);//Init queue buffer
       printf("Init buffer #%d\n", i);
    }
    mr->buffer = (struct queue_t *)malloc(sizeof(arrayq));
    memcpy(mr->buffer, arrayq, sizeof(arrayq));

    //Allocate memory for array of locks
    mr->empty = (pthread_cond_t *)malloc(mr->threads*sizeof(pthread_cond_t));
    mr->fill = (pthread_cond_t *)malloc(mr->threads*sizeof(pthread_cond_t));
    mr->mutexes = (pthread_mutex_t *)malloc(mr->threads*sizeof(pthread_mutex_t));
    //Create locks
    pthread_cond_t emptys[mr->threads];
    pthread_cond_t fills[mr->threads];
    pthread_mutex_t mutexes[mr->threads];   
    for (i=0; i <mr->threads; i++){
        pthread_cond_init(&emptys[i], NULL);
        pthread_cond_init(&fills[i], NULL);
        pthread_mutex_init(&mutexes[i], NULL); 
    }
    memcpy(mr->empty, emptys, sizeof(emptys));
    memcpy(mr->fill, fills, sizeof(fills));
    memcpy(mr->mutexes, mutexes, sizeof(mutexes));
    
    printf("Finished initialization\n");
    return mr;
}

void mr_destroy(struct map_reduce *mr) {
    printf("Freeing allocated memory\n");
    free(mr->map_tid);//Free mapper tid array
    free(mr->mfd);    //Free mapper fd array
    free(mr->join);   //Free mapper threads status array
    free(mr->buffer); //Free buffer 
    free(mr->empty);  //Free locks
    free(mr->fill);
    free(mr->mutexes);
    free(mr);         //Free map_reduce struct
    mr = NULL;
    printf("Memory freed\n");
}

int mr_start(struct map_reduce *mr, const char *inpath, const char *outpath) {
    printf("Starting threads with:\n");
    printf("Input file path: %s\nOutput file path: %s\n", inpath, outpath);
    
    int i; // 'for' loop iterator
    pthread_t mthread_id[mr->threads]; //Hold mapper thread id 
    pthread_t rthread_id; //Hold producer thread id 
    int m_fd[mr->threads]; //Hold mapper fds

    for (i = 0; i<mr->threads; i++) {
        //Initialize map_fn parameter struct
        struct map_param *mp = (struct map_param *)malloc(sizeof(struct map_param));
        mp->mr = mr;
        mp->nmaps = mr->threads;
        mp->mapID = i;
        
        //Input file descriptor
        int infd = open(inpath, O_RDONLY, S_IRUSR);
        if (infd == -1) {
            printf("Error number %d\n", errno);
            printf("Input fd error\n");
            return 1;
        }
        mp->infd = infd;
        m_fd[i] = infd; //Put input fd to array
        
        //Create mapper threads
        int pc = pthread_create(&mthread_id[i], NULL, &Map_fn, (void *)mp);
        if (pc) {
            printf("Error number: %d\n", errno);
            printf("Unable to create thread %d\n", i);
            return 1;
        }
        mr->join[mp->mapID] = 0;
    }
   
    //Copy tid and fd to mr struct  
    memcpy(mr->map_tid, mthread_id, mr->threads*sizeof(pthread_t));
    memcpy(mr->mfd, m_fd, mr->threads*sizeof(int));

    //Ouput file descriptor
    int outfd = open(outpath, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (outfd == -1) {
        printf("Error number %d\n", errno);
        printf("Output fd error\n");
        return 1;
    }
    mr->rfd = outfd; //Store the reducer fd to map_reduce struct

    //Initialize reduce_fn parameter struct
    struct reduce_param *rp = (struct reduce_param *)malloc(sizeof(struct reduce_param));
    rp->mr = mr;
    rp->outfd = outfd;
    rp->nmaps = mr->threads;
    //Create reduce thread
    int pc = pthread_create(&rthread_id, NULL, &Reduce_fn, (void *)rp);
    if (pc) {
        printf("Error number: %d\n", errno);
        printf("Unable to create reduce thread\n");
        return 1;
    }
    mr->reduce_tid = rthread_id; //Save reducer tid in mr struct 
 
    printf("Threads created\n");
    return 0;
}

int mr_finish(struct map_reduce *mr) {
    printf("Waiting for threads to finish\n");
    int i; // 'for' loop iterator
    void *ret; //Access return value of mapper and reducer 

    //Wait for mappers
    for (i = 0; i<mr->threads; i++) {
        if (pthread_join(mr->map_tid[i], &ret) != 0) {
            printf("Error number: %d\n", errno);
            printf("Join t#%d error\n", i);
        }
        if (*(int *)ret != 0) //Check return value of map funcitons
            return 2; 

        printf("Thread %d finished\n", i);
    }
    
    //Wait for reducer
    if (pthread_join(mr->reduce_tid, &ret) != 0) {
        printf("Error number: %d\n", errno);
        printf("Join reduce error\n");
    }

    if (*(int *)ret != 0) //Check return value of reduce function
        return 3;

    //Close all the fd of mappers
    for (i = 0; i<mr->threads; i++) {
        if (close(*(mr->mfd+i)) == -1) {
            printf("Close mapper fd error with %d\n", errno);
        }
    }
    //Close reducer fd
    if (close(mr->rfd) == -1) {
        printf("Close reduce fd error with %d\n", errno);
    }

    printf("All threads are finished\n");
    return 0;
}

int mr_produce(struct map_reduce *mr, int id, const struct kvpair *kv) {
    int kv_size = 2*sizeof(uint32_t) + kv->keysz + kv->valuesz;
    struct queue_t *sbuffer = &mr->buffer[id]; 
    pthread_mutex_t *mutex = &mr->mutexes[id];
    pthread_cond_t *fill = &mr->fill[id];
    pthread_cond_t *empty = &mr->empty[id];
    
    pthread_mutex_lock(mutex); 
    if (kv_size > mr->buffer_size) //If key-value larger than the whole buffer, fail
        return -1;

    while ((sbuffer->size - kv_size) <= 0) { //If no enough size in buffer, wait 
        if (pthread_cond_wait(empty, mutex))
           return -1;
    }

    enqueue(sbuffer, mr, kv);//Put key-value pair into queue buffer
    
    if (pthread_cond_signal(fill))
        return -1;  
    
    pthread_mutex_unlock(mutex);   
    
    return 1;
}

int mr_consume(struct map_reduce *mr, int id, struct kvpair *kv) {
    struct queue_t *sbuffer = &mr->buffer[id];
    pthread_mutex_t *mutex = &mr->mutexes[id];
    pthread_cond_t *fill = &mr->fill[id];
    pthread_cond_t *empty = &mr->empty[id]; 
    
    pthread_mutex_lock(mutex); 
    //check if buffer is empty
    while (sbuffer->count == 0) {
        if (mr->join[id] != 0) //If this mapper has exited and buffer is empty, return 0
            return 0;          //If join[id] is 0, then thread is running; nonzero for exited   
        
        if (pthread_cond_wait(fill, mutex))
            return -1;
    }

    dequeue(mr->buffer+id, kv); //Get key-value pair from buffer
    
    if (pthread_cond_signal(empty)) //Signal producer
        return -1;
    
    pthread_mutex_unlock(mutex);
    
    return 1;
}
