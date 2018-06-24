/*
 * External merge sorting of big file.
 *
 * WARNING: Works only when buffer size is aliquot part of file size!
 *          This is for simplicity of implementation.
 *
 * Copyright (c) 2015 Alex Dzyoba <avd@reduct.ru>
 *
 * This project is licensed under the terms of the MIT license
 */
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
using namespace std;

struct lessKMerge{
    bool operator()(pair<ifstream*,string> lhs, pair<ifstream*,string> rhs){
        return  lhs.second.compare(rhs.second)<0;
    }
};

struct lessString{
    bool operator()(string lhs, string rhs){


        return lexicographical_compare(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
//        return lhs.compare(rhs)<0;
    }
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
    cerr << "Save chunk " << chunk << " size=" << data.size()/pow(pow(2,10),3) << "GB to " << path << endl;
    cerr << "-----------------------------------------------\n";
    ofstream of(path, ofstream::binary);
    int data_size = data.size();
    copy(data.begin(), data.begin()+data_size-1, ostream_iterator<string>(of, " "));
    of << data[data_size-1];
    of.close();

//exit:
    if (path) {
        free(path);
    }
    return data_size;
}

// Return chunks number of -1 on error
int split(ifstream *f, off_t filesize, char *dir, size_t &max_word_length, size_t bufsize)
{
    int size_of_text = 0;
    vector<string> data;
    string word;
    int i = 0;
    while(!f->eof())
    {
        *f >> word;
        if(word.empty())
            cerr << "+";
        if(word.size() > max_word_length)
            max_word_length = word.size();

        if(size_of_text+word.size() < bufsize){
            data.push_back(word);
            size_of_text++;
            size_of_text+= word.size();
        }else{
            cerr << i << ":";
            cerr << "sorting data size=" << data.size() << "~" << size_of_text/pow(pow(2,10),3) << "GB" << endl;
            clock_t start_temp = clock();
            std::make_heap(data.begin(), data.end());
            std::sort_heap(data.begin(), data.end());
            clock_t end_temp = clock();
            cerr << "complete sort! in " << (end_temp-start_temp)/CLOCKS_PER_SEC << "secs" << endl;
            save_buf(data, dir, i);

            i++;
            data.clear();
            data.push_back(word);
            size_of_text = word.size();
        }
    }
    if(!data.empty()){
        cerr << i << ":";
        cerr << "sorting data size=" << data.size() << "~" << size_of_text/pow(pow(2,10),3) << "GB" << endl;
        clock_t start_temp = clock();
        std::make_heap(data.begin(), data.end());
        std::sort_heap(data.begin(), data.end());
        clock_t end_temp = clock();
        cerr << "complete sort! in " << (end_temp-start_temp)/CLOCKS_PER_SEC << "secs" << endl;
        save_buf(data, dir, i);

        i++;
        data.clear();
    }
    f->close();
    delete f;
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
            cerr << "Remove a chunk out of data! ==>" << k_merge.size() << " chunks remain data!";
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
    return 0;
}

int main(int argc, const char *argv[])
{
    size_t mem = sysconf(_SC_AVPHYS_PAGES)*sysconf(_SC_PAGESIZE);
    ifstream *f;
    char *dirpath;
    size_t bufsize;
    char dirname[] = "/media/nguyennd/win/test/tmp.XXXXXX";
    struct stat sb;
    off_t file_size;
    clock_t start, end;

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

    cerr << "File size = " << file_size/pow(pow(2,10),3) << "GB,  mem = " << mem/pow(pow(2,10),3) << "GB" << endl;

    if(file_size < (off_t)mem/3){
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



    bufsize = mem/8;
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

err:
    return 0;
}
