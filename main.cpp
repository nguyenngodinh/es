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

#define max_thread 8
#define buffer_factor 32
#define internal_factor 3

#define Byte2GB(byteValue) byteValue/pow(pow(2,1024),3)
#define AvailabeMem sysconf(_SC_AVPHYS_PAGES)*sysconf(_SC_PAGESIZE)
#define TotalMem sysconf(_SC_PHYS_PAGES)*sysconf(_SC_PAGESIZE)
#define AvailableMemRate AvailabeMem*1.0/TotalMem

#define devmode 0


using namespace std;

pthread_mutex_t lock_running_thread;
int number_running_thread = 0;

void logout_mem_available(){
#if (devmode == 1)
    cerr << "Mem available: " << AvailableMemRate << "% over total is " << TotalMem << "GB." << endl;
#endif
}
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
    while (d){
        d /= 10;
        filelen++;
    }

    // One for '/' and one for '\0'
    path_len = dirlen + filelen + 1 + 1;
    path = new char[path_len];
    snprintf(path, path_len, "%s/%d", dir, chunk);

    return path;
}

int save_buffer(const vector<string>& data, char *dir, int chunk)
{
    char *path;
    path = form_filename(chunk, dir);
#if (devmode == 1)
    cerr << "Save chunk " << chunk << " contains " << data.size()<< " words to " << path << endl;
#endif
    logout_mem_available();
    cerr << "-----------------------------------------------\n";
    ofstream of(path, ofstream::binary);
    int data_size = data.size();
    copy(data.begin(), data.begin()+data_size-1, ostream_iterator<string>(of, " "));
    of << data[data_size-1];
    of.close();

    if (path) {
        free(path);
    }
    return data_size;
}

void *sort_task(void* arg){
    chunk_data* chunk = (chunk_data*)arg;
#if (devmode == 1)
    cerr << "Chunk" << chunk->chunkId << ".Start process "<< chunk->data.size() << " words.ThreadId="<< (unsigned int)pthread_self()  << endl;
#endif
    clock_t start_task = clock();
    std::make_heap(chunk->data.begin(), chunk->data.end());
    std::sort_heap(chunk->data.begin(), chunk->data.end());
#if (devmode == 1)
    cerr << "Chunk" << chunk->chunkId << ".Done the sort task in " << (double)(clock()-start_task)/CLOCKS_PER_SEC << "secs\n";
    start_task = clock();
#endif
    save_buffer(chunk->data, chunk->dir, chunk->chunkId);
#if (devmode == 1)
    cerr << "Chunk" << chunk->chunkId << ".Done the write task in " << (double)(clock()-start_task)/CLOCKS_PER_SEC << "secs\n";
#endif
    pthread_mutex_lock(&lock_running_thread);
    number_running_thread-=1;
    pthread_mutex_unlock(&lock_running_thread);
    chunk->data.clear();

    delete chunk;
    pthread_exit(NULL);
}

// Return chunks number of -1 on error
int split(ifstream *f, off_t filesize, char *dir, size_t &max_word_length, size_t bufsize)
{    
    pthread_mutex_init(&lock_running_thread,NULL);
    int size_of_text = 0;
    chunk_data* chunk = new chunk_data;
    string word;
    int i = 0;
    clock_t start_read = clock();
    while(!f->eof())
    {
        *f >> word;
#if (devmode ==1)
        if(word.empty())
            cerr << "+";
#endif
        if(word.size() > max_word_length)
            max_word_length = word.size();

        if(size_of_text+word.size() < bufsize){
            chunk->data.push_back(word);
            size_of_text++;
            size_of_text+= word.size();
        }else{
#if (devmode == 1)
            cerr << i << ": IO time = " << (clock()-start_read)/CLOCKS_PER_SEC << " secs. ThreadId=" << (unsigned int)pthread_self() << endl;
#endif
            chunk->chunkId = i;
            chunk->dir = dir;
            chunk->mem = size_of_text;
            bool overthread;
            do{
                overthread = false;
                pthread_mutex_lock(&lock_running_thread);
                if(number_running_thread >= max_thread)
                    overthread =true;
                pthread_mutex_unlock(&lock_running_thread);
                if(overthread){
                    usleep(1000);
#if (devmode == 1)
                    cerr << "Thread is reach to " << max_thread << endl;
#endif
                }
                else{
                    int result = -1;
                    do{
                        result = pthread_create(new pthread_t, NULL, sort_task, (void*)chunk);
                        if(result==0){
                            pthread_mutex_lock(&lock_running_thread);
                            number_running_thread+=1;
                            pthread_mutex_unlock(&lock_running_thread);
                        }

                    }while(result!=0);
                }
            }while(overthread);


            i++;
            start_read = clock();
            size_of_text = word.size();
            chunk = new chunk_data;
            chunk->data.push_back(word);
        }
    }
    if(!chunk->data.empty()){
#if (devmode == 1)
        cerr << i << ": IO time = " << (clock()-start_read)/CLOCKS_PER_SEC << " secs.\n";
#endif
        chunk->chunkId = i;
        chunk->dir = dir;
        chunk->mem = size_of_text;
        if(number_running_thread <max_thread){
            usleep(1000);
            int result = -1;
            do{
                result = pthread_create(new pthread_t, NULL, sort_task, (void*)chunk);
                if(result==0){
                    pthread_mutex_lock(&lock_running_thread);
                    number_running_thread+=1;
                    pthread_mutex_unlock(&lock_running_thread);
                }

            }while(result!=0);
        }
        i++;
    }
    f->close();
    delete f;
    bool isComplete = false;
#if (devmode == 1)
    cerr << "Wait to read all data:";
#endif
    int currentRunning =-1;
    while(!isComplete){
        pthread_mutex_lock(&lock_running_thread);
        if(number_running_thread ==0)
           isComplete = true;
        pthread_mutex_unlock(&lock_running_thread);
        if(currentRunning != number_running_thread)
        {
            currentRunning = number_running_thread;
#if (devmode == 1)
            cerr << "Remain " << currentRunning << " threads!" << endl;
#endif
        }
        usleep(1000);
    }
#if (devmode == 1)
    cerr << "Complete split " << Byte2GB(filesize) << "GB file to " << i << " chunk file of size " << Byte2GB(bufsize) << "GB" << endl;
#endif
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
        ifstream* chunkfs = new ifstream(form_filename(i, dir), ifstream::binary);
        string word;
        (*chunkfs) >> word;
        chunk_fs_ptrs.push_back(chunkfs);
        pair<ifstream*, string> value(chunkfs,word);
        k_merge.insert(value);
    }
    cerr << "Init " << chunks << "-merge" << endl;
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
#if (devmode == 1)
            cerr << "Remove a chunk out of data! ==>" << k_merge.size() << " chunks remain data!" << endl;
#endif
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
    size_t available_memory = sysconf(_SC_AVPHYS_PAGES)*sysconf(_SC_PAGESIZE);
    ifstream *input_file_stream;
    char *dirpath;
    size_t bufsize;
    char dirname[] = "tmp.XXXXXX";
    struct stat sb;
    off_t file_size;
    clock_t start, end;


    if (argc != 2) {
        fprintf(stderr, "Usage: %s <file to sort>\n. Note: The result will be in cout (standard output).\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    errno = 0;
    if (stat(argv[1], &sb)) {
        perror("stat");
        exit(EXIT_FAILURE);
    }


    file_size = sb.st_size;

    cerr << "File size = " << file_size/pow(pow(2,10),3) << "GB,  available mem = " << available_memory/pow(pow(2,10),3) << "GB" << endl;

    if(file_size < (off_t)available_memory/internal_factor){
        start = clock();
        ifstream file(argv[1]);
        std::vector<string> data;
        cerr << "User iternal sort:" << endl;
        cerr << "Start reading file!" << endl;
        while(!file.eof()){
            string word;
            file >> word;
            data.push_back(word);
        }
        cerr << "Finish reading file!" << endl;
        std::make_heap(data.begin(), data.end());
        std::sort_heap(data.begin(), data.end());
        cerr << "Finish sorting data!" << endl;
        copy(data.begin(), data.end(), ostream_iterator<string>(cout, "\n"));
        end = clock();
        cerr << file_size/pow(pow(2,10),3) << "GB sorted in " << (double)(end-start)/CLOCKS_PER_SEC << endl;
        return 0;
    }

    cerr << "Use external sort:" << endl;
    bufsize = available_memory/buffer_factor;

    cerr << "Buffer size=" << bufsize/pow(pow(2,10),3) << "GB,  mem available=" << available_memory/pow(pow(2,10),3) << "GB" <<endl;
    cerr << "Approximately use " << file_size/bufsize << " chunks" << endl;

    input_file_stream = new ifstream(argv[1]);
    if (input_file_stream == NULL) {
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
    if (external_merge_sort(input_file_stream, file_size, dirpath, bufsize)) {
        fprintf(stderr, "Failed to sort %s\n", argv[1]);
        goto err;
    }
    end = clock();
#if (devmode == 1)
    cerr << file_size/pow(pow(2,10),3) << "GB sorted in " << (double)(end-start)/CLOCKS_PER_SEC << endl;
#endif

err:
    return 0;
}
