#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <iterator>
#include <unistd.h>
#include <set>
#include <functional>
#include <math.h>
#include <unistd.h>
#include <pthread.h>
using namespace std;


#define MAX_THREADS 64
#define MAX_QUEUE 65536


pthread_mutex_t lock;
pthread_mutex_t lock_mem_read;
int task=0;
int done=0;
std::size_t mem_read=0;

void dummy_task(void *arg){
    int *i = (int*)arg;
    cerr << *i << ".enter task! then sleep!\n";
    usleep(1000);
    cerr << *i << ".awake\n";
    pthread_mutex_lock(&lock);
    done++;
    cerr << *i << ".done a task\n";
    pthread_mutex_unlock(&lock);
}

void logout_mem_available(){
    cerr << "Mem available: " << (sysconf(_SC_AVPHYS_PAGES)*1.0)/sysconf(_SC_PHYS_PAGES) << "% over total is " << sysconf(_SC_PHYS_PAGES)*sysconf(_SC_PAGESIZE)/pow(pow(2,10),3) << "GB." << endl;
}

/*
 * @brief Thread pool iterface definition
 */

typedef struct threadpool_t threadpool_t;
typedef enum {
    threadpool_invalid        = -1,
    threadpool_lock_failure   = -2,
    threadpool_queue_full     = -3,
    threadpool_shutdown       = -4,
    threadpool_thread_failure = -5
} threadpool_error_t;
typedef enum {
    immediate_shutdown = 1,
    graceful_shutdown  = 2
} threadpool_shutdown_t;
typedef struct {
    void (*function)(void *);
    void *argument;
} threadpool_task_t;
struct threadpool_t {
    pthread_mutex_t lock;
    pthread_cond_t notify;
    pthread_t *threads;
    threadpool_task_t *queue;
    int thread_count;
    int queue_size;
    int head;
    int tail;
    int count;
    int shutdown;
    int started;
};
typedef enum {
    threadpool_graceful       = 1
} threadpool_destroy_flags_t;
static void *threadpool_thread(void *threadpool);
int threadpool_destroy(threadpool_t *pool, int flags);
int threadpool_free(threadpool_t *pool);
threadpool_t* threadpool_create(int thread_count, int queue_size, int flags);
int threadpool_add(threadpool_t *pool, void (*function)(void *), void *arg, int flags);

/**
 * @brief The lessKMerge struct
 */
struct lessKMerge{
    bool operator()(pair<ifstream*,string> lhs, pair<ifstream*,string> rhs){
        return  lhs.second.compare(rhs.second)<0;
    }
};

struct chunk_data{
    int chunkId;
    char* dir;
    std::size_t mem;
    vector<string> data;
};

threadpool_t *pool;


char *form_filename(int chunk, char *dir)
{
    char *path;
    int d;
    int dirlen, filelen, path_len;

    dirlen = strlen(dir);

    // File name is a chunk number, so
    // number of digits in chunk is a filename length
    filelen = 1;
    d = chunk;
    while (d)
    {
        d /= 10;
        filelen++;
    }

    // One for '/' and one for '\0'
    path_len = dirlen + filelen + 1 + 1;
    path = new char[path_len];
    snprintf(path, path_len, "%s/%d", dir, chunk);

    return path;
}

int save_buf(const vector<string>& data, char *dir, int chunk)
{
    char *path;
    path = form_filename(chunk, dir);
    cerr << "Save chunk " << chunk << " contains " << data.size()<< " words to " << path << endl;
    logout_mem_available();
    cerr << "-----------------------------------------------\n";
//    pthread_mutex_lock(&lock);
    ofstream of(path, ofstream::binary);
    int data_size = data.size();
    copy(data.begin(), data.begin()+data_size-1, ostream_iterator<string>(of, " "));
    of << data[data_size-1];
    of.close();
//    pthread_mutex_unlock(&lock);

    //exit:
    if (path) {
        free(path);
    }
    return data_size;
}

void sort_task(void* arg){
    chunk_data* chunk = (chunk_data*)arg;
    cerr << "Chunk" << chunk->chunkId << ".Start process "<< chunk->data.size() << " words."<< endl;
    clock_t start_task = clock();
    std::make_heap(chunk->data.begin(), chunk->data.end());
    std::sort_heap(chunk->data.begin(), chunk->data.end());
    cerr << "Chunk" << chunk->chunkId << ".Done the sort task in " << (double)(clock()-start_task)/CLOCKS_PER_SEC << "secs\n";
    start_task = clock();
    save_buf(chunk->data, chunk->dir, chunk->chunkId);
    cerr << "Chunk" << chunk->chunkId << ".Done the write task in " << (double)(clock()-start_task)/CLOCKS_PER_SEC << "secs\n";
    pthread_mutex_lock(&lock_mem_read);
    mem_read += chunk->mem;
    cerr << "read " << mem_read/pow(pow(2,10),3) << "GB. in chunk " << chunk->chunkId << endl;
    pthread_mutex_unlock(&lock_mem_read);
    chunk->data.clear();
    delete chunk;
    cerr << "Chunk" << chunk->chunkId << "End!" << endl;
    pthread_exit(NULL);
}

// Return chunks number of -1 on error
int split(ifstream *f, off_t filesize, char *dir, size_t &max_word_length, size_t bufsize)
{
    pthread_mutex_init(&lock,NULL);

    int size_of_text = 0;
    chunk_data* chunk = new chunk_data;
//    vector<string> data;
    string word;
    int i = 0;
    clock_t start_read = clock();
    while(!f->eof())
    {
        *f >> word;
        if(word.empty())
            cerr << "+";
        if(word.size() > max_word_length)
            max_word_length = word.size();

        if(size_of_text+word.size() < bufsize){
//            data.push_back(word);
            chunk->data.push_back(word);
            size_of_text++;
            size_of_text+= word.size();
        }else{
            cerr << i << ": IO time = " << (clock()-start_read)/CLOCKS_PER_SEC << " secs.\n";
            cerr << "Sorting " << chunk->data.size() << " words as " << size_of_text/pow(pow(2,10),3) << "GB" << endl;
//            logout_mem_available();
            chunk->chunkId = i;
            chunk->dir = dir;
            chunk->mem = size_of_text;
            int result = -1;
            logout_mem_available();
            cerr << "Wait for available thread ";
            do{
                double rate = (1.0*sysconf(_SC_AVPHYS_PAGES))/sysconf(_SC_PHYS_PAGES);
                if(rate <0.40)
                {
                    cerr << ".";
                    usleep(2000);
                }
                result = threadpool_add(pool, &sort_task, (void*)chunk, 0);
                if(result!=0){
                    cerr << result;
                    usleep(2000);
                }else{
                    cerr << "\nCreate thread to do the task with chunk id " << chunk->chunkId << endl;
                }

            }while(result!=0);


//            clock_t start_temp = clock();
//            if(i%2==0){
//                cerr << "Heap_sort -> ";
//                std::make_heap(data.begin(), data.end());
//                std::sort_heap(data.begin(), data.end());
//            }
//            else{
//                cerr << "Sort -> ";
//                std::sort(data.begin(), data.end());
//            }
//            clock_t end_temp = clock();
//            cerr << "Complete sort! in " << (end_temp-start_temp)/CLOCKS_PER_SEC << "secs" << endl;
//            save_buf(data, dir, i);

            i++;
//            data.clear();
//            data.push_back(word);
            size_of_text = word.size();
//            start_read = clock();

            chunk = new chunk_data;
            chunk->data.push_back(word);
        }
    }
    if(!chunk->data.empty()){
        cerr << i << ":";
        cerr << "sorting data size=" << chunk->data.size() << "~" << size_of_text/pow(pow(2,10),3) << "GB" << endl;
        chunk->chunkId = i;
        chunk->dir = dir;
        chunk->mem = size_of_text;
        int result = -1;
        do{
            result = threadpool_add(pool, &sort_task, (void*)chunk, 0);
            if(result!=0)
                usleep(1000);

        }while(result!=0);

//        clock_t start_temp = clock();
//        std::make_heap(data.begin(), data.end());
//        std::sort_heap(data.begin(), data.end());
//        clock_t end_temp = clock();
//        cerr << "complete sort! in " << (end_temp-start_temp)/CLOCKS_PER_SEC << "secs" << endl;
//        save_buf(data, dir, i);



        i++;
//        data.clear();
    }
    f->close();
    delete f;
    cerr << "Wait to read all data:";
    while(mem_read < filesize){
//        cerr << ".";
        usleep(1000);
    }

    cerr << "Complete split " << filesize/pow(pow(2,10),3) << "GB file to " << i << " chunk file of size " << bufsize/pow(10,9) << "GB" << endl;
    return i;
}

int external_merge_sort(ifstream *f, off_t filesize, char *dir, size_t bufsize)
{
    int chunks;
    char *buf;
    size_t max_word_length = 0;
    size_t max_store_elem = 0;

    buf = new char[bufsize];
    if (!buf) {
        perror("new");
        return -ENOMEM;
    }
    // Phase 1: split file to sorted chunks of size bufsize.
    chunks = split(f, filesize, dir, max_word_length, bufsize);

    size_t mem = sysconf(_SC_AVPHYS_PAGES)*sysconf(_SC_PAGESIZE);
    max_store_elem = mem*0.7 - (filesize/bufsize)*max_word_length;

    cerr << "Start merge " << chunks << " chunks with max store buffer =" << max_store_elem/pow(10,9) << "GB" << endl;
    // Phase 2: merge chunks.
    multiset<pair<ifstream*, string>, lessKMerge> k_merge;//<chunk_fs,text_value>
    vector<ifstream*> chunk_fs_ptrs;
    for(int i=0; i<chunks; i++){
        ifstream* fs = new ifstream(form_filename(i, dir), ifstream::binary);
        string word;
        (*fs) >> word;
        chunk_fs_ptrs.push_back(fs);
        pair<ifstream*, string> value(fs,word);
        k_merge.insert(value);
    }
    int count=0;
    int size_of_text = 0;
    vector<string> data;
    while(!k_merge.empty()){
        if(size_of_text +k_merge.begin()->second.size() < max_store_elem){
            data.push_back(k_merge.begin()->second);
            size_of_text += k_merge.begin()->second.size();
        }
        else{
            cerr << "Push out " << size_of_text/pow(pow(2,10),3) << "GB as ";
            copy(data.begin(), data.end(), ostream_iterator<string>(cout, "\n"));
            cerr << "sorted " << count << " words" << endl;
            data.clear();
            data.push_back(k_merge.begin()->second);
            size_of_text = k_merge.begin()->second.size();
        }
        count++;
        ifstream* fs = k_merge.begin()->first;
        if(fs->eof()){
            cerr << "Remove a chunk out of data! ==>" << k_merge.size() << " chunks remain data!" << endl;
            k_merge.erase(k_merge.begin());
            delete fs;
            continue;
        }
        string word;
        (*fs) >> word;
        k_merge.erase(k_merge.begin());
        pair<ifstream*, string> temp(fs, word);
        k_merge.insert(temp);
    }
    if(!data.empty()){
        cerr << "Push out " << size_of_text/pow(pow(2,10),3) << "GB as ";
        copy(data.begin(), data.end(), ostream_iterator<string>(cout, "\n"));
        cerr << "sorted " << count << " words" << endl;
        data.clear();
    }

    return 0;
}

int main(int argc, const char *argv[])
{

    /*
    pthread_mutex_init(&lock, NULL);
    threadpool_t *pool = threadpool_create(2, 4, 0);
    cerr << "pool start with size thread = 10, queue=256" << endl;
    int result = 1;
    int i=1;
    while(i!=32){
        do{
            result = threadpool_add(pool, &dummy_task, (void*)&i, 0);
            if(result!=0)
                usleep(2000);
        }while(result!=0);
        i++;
    }
    cerr << "Added " << task << " tasks." << endl;
    while(true){
        usleep(10000);
    }
    threadpool_destroy(pool, 0);
    cerr << "Did " << done << " tasks." << endl;

    return 1;
*/

    size_t mem = sysconf(_SC_AVPHYS_PAGES)*sysconf(_SC_PAGESIZE);
    ifstream *f;
    char *dirpath;
    size_t bufsize;
    char dirname[] = "tmp.XXXXXX";
    struct stat sb;
    off_t file_size;
    clock_t start, end;


    //create pool
    pool = threadpool_create(8,6,0);

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <file to sort>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    errno = 0;
    if (stat(argv[1], &sb)) {
        perror("stat");
        exit(EXIT_FAILURE);
    }


    file_size = sb.st_size;

    cerr << "File size = " << file_size/pow(pow(2,10),3) << "GB,  available mem = " << mem/pow(pow(2,10),3) << "GB" << endl;

//    if(file_size < (off_t)mem/3){
//        start = clock();
//        ifstream file(argv[1]);
//        std::vector<string> data;
//        cerr << "User iternal sort:" << endl;
//        cerr << "Start reading file!" << endl;
//        while(!file.eof()){
//            string word;
//            file >> word;
//            data.push_back(word);
//        }
//        cerr << "Finish reading file!" << endl;
//        std::make_heap(data.begin(), data.end());
//        std::sort_heap(data.begin(), data.end());
//        cerr << "Finish sorting data!" << endl;
//        copy(data.begin(), data.end(), ostream_iterator<string>(cout, "\n"));
//        end = clock();
//        cerr << file_size/pow(pow(2,10),3) << "GB sorted in " << (double)(end-start)/CLOCKS_PER_SEC << endl;
//        return 0;
//    }



    bufsize = mem/32;
    std::cerr << "Buffer size=" << bufsize/pow(pow(2,10),3) << "GB,  mem available=" << mem/pow(pow(2,10),3) << "GB" <<endl;
    cerr << "Approximately use " << file_size/bufsize << " chunks" << endl;

    f = new ifstream(argv[1]);
    if (f == NULL) {
        perror("fopen");
        exit(EXIT_FAILURE);
    }

    // Create temp dir
    dirpath = mkdtemp(dirname);
    if (dirpath == NULL) {
        perror("mkdtemp");
        goto err;
    }

    // Do stuff
    start = clock();
    if (external_merge_sort(f, file_size, dirpath, bufsize)) {
        fprintf(stderr, "Failed to sort %s\n", argv[1]);
        goto err;
    }
    end = clock();
    cerr << file_size/pow(pow(2,10),3) << "GB sorted in " << (double)(end-start)/CLOCKS_PER_SEC << endl;

    //destroy pool
    threadpool_destroy(pool,0);
err:
    return 0;
}



threadpool_t *threadpool_create(int thread_count, int queue_size, int flags)
{
    threadpool_t *pool;
    int i;
    (void) flags;

    if(thread_count <= 0 || thread_count > MAX_THREADS || queue_size <= 0 || queue_size > MAX_QUEUE) {
        return NULL;
    }

    if((pool = (threadpool_t *)malloc(sizeof(threadpool_t))) == NULL) {
        goto err;
    }

    /* Initialize */
    pool->thread_count = 0;
    pool->queue_size = queue_size;
    pool->head = pool->tail = pool->count = 0;
    pool->shutdown = pool->started = 0;

    /* Allocate thread and task queue */
    pool->threads = (pthread_t *)malloc(sizeof(pthread_t) * thread_count);
    pool->queue = (threadpool_task_t *)malloc
            (sizeof(threadpool_task_t) * queue_size);

    /* Initialize mutex and conditional variable first */
    if((pthread_mutex_init(&(pool->lock), NULL) != 0) ||
            (pthread_cond_init(&(pool->notify), NULL) != 0) ||
            (pool->threads == NULL) ||
            (pool->queue == NULL)) {
        goto err;
    }

    /* Start worker threads */
    for(i = 0; i < thread_count; i++) {
        if(pthread_create(&(pool->threads[i]), NULL,
                          threadpool_thread, (void*)pool) != 0) {
            threadpool_destroy(pool, 0);
            return NULL;
        }
        pool->thread_count++;
        pool->started++;
    }

    return pool;

err:
    if(pool) {
        threadpool_free(pool);
    }
    return NULL;
}
int threadpool_add(threadpool_t *pool, void (*function)(void *),
                   void *arg, int flags){
    int err = 0;
    int next;
    (void) flags;

    if(pool == NULL || function == NULL) {
        return threadpool_invalid;
    }

    if(pthread_mutex_lock(&(pool->lock)) != 0) {
        return threadpool_lock_failure;
    }

    next = (pool->tail + 1) % pool->queue_size;

    do {
        /* Are we full ? */
        if(pool->count == pool->queue_size) {
            err = threadpool_queue_full;
            break;
        }

        /* Are we shutting down ? */
        if(pool->shutdown) {
            err = threadpool_shutdown;
            break;
        }

        /* Add task to queue */
        pool->queue[pool->tail].function = function;
        pool->queue[pool->tail].argument = arg;
        pool->tail = next;
        pool->count += 1;

        /* pthread_cond_broadcast */
        if(pthread_cond_signal(&(pool->notify)) != 0) {
            err = threadpool_lock_failure;
            break;
        }
    } while(0);

    if(pthread_mutex_unlock(&pool->lock) != 0) {
        err = threadpool_lock_failure;
    }

    return err;
}
int threadpool_destroy(threadpool_t *pool, int flags){
    int i, err = 0;

    if(pool == NULL) {
        return threadpool_invalid;
    }

    if(pthread_mutex_lock(&(pool->lock)) != 0) {
        return threadpool_lock_failure;
    }

    do {
        /* Already shutting down */
        if(pool->shutdown) {
            err = threadpool_shutdown;
            break;
        }

        pool->shutdown = (flags & threadpool_graceful) ?
                    graceful_shutdown : immediate_shutdown;

        /* Wake up all worker threads */
        if((pthread_cond_broadcast(&(pool->notify)) != 0) ||
                (pthread_mutex_unlock(&(pool->lock)) != 0)) {
            err = threadpool_lock_failure;
            break;
        }

        /* Join all worker thread */
        for(i = 0; i < pool->thread_count; i++) {
            if(pthread_join(pool->threads[i], NULL) != 0) {
                err = threadpool_thread_failure;
            }
        }
    } while(0);

    /* Only if everything went well do we deallocate the pool */
    if(!err) {
        threadpool_free(pool);
    }
    return err;
}
int threadpool_free(threadpool_t *pool){
    if(pool == NULL || pool->started > 0) {
        return -1;
    }

    /* Did we manage to allocate ? */
    if(pool->threads) {
        free(pool->threads);
        free(pool->queue);

        /* Because we allocate pool->threads after initializing the
           mutex and condition variable, we're sure they're
           initialized. Let's lock the mutex just in case. */
        pthread_mutex_lock(&(pool->lock));
        pthread_mutex_destroy(&(pool->lock));
        pthread_cond_destroy(&(pool->notify));
    }
    free(pool);
    return 0;
}

static void *threadpool_thread(void *threadpool)
{
    threadpool_t *pool = (threadpool_t *)threadpool;
    threadpool_task_t task;

    for(;;) {
        /* Lock must be taken to wait on conditional variable */
        pthread_mutex_lock(&(pool->lock));

        /* Wait on condition variable, check for spurious wakeups.
           When returning from pthread_cond_wait(), we own the lock. */
        while((pool->count == 0) && (!pool->shutdown)) {
            pthread_cond_wait(&(pool->notify), &(pool->lock));
        }

        if((pool->shutdown == immediate_shutdown) ||
                ((pool->shutdown == graceful_shutdown) &&
                 (pool->count == 0))) {
            break;
        }

        /* Grab our task */
        task.function = pool->queue[pool->head].function;
        task.argument = pool->queue[pool->head].argument;
        pool->head = (pool->head + 1) % pool->queue_size;
        pool->count -= 1;

        /* Unlock */
        pthread_mutex_unlock(&(pool->lock));

        cerr << "New task !" << endl;
        /* Get to work */
        (*(task.function))(task.argument);
    }

    pool->started--;

    pthread_mutex_unlock(&(pool->lock));
    pthread_exit(NULL);
    return(NULL);
}


