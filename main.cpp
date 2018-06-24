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

using namespace std;


void print_arr(int A[], int n)
{
    int i;
    for (i = 0; i < n; i++)
        printf("%d\n", A[i]);
}

int compar(const void *p1, const void *p2)
{
    int *i1, *i2;

    i1 = (int *)p1;
    i2 = (int *)p2;

    return *i1 - *i2;
}

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
    cout << "Save chunk " << chunk << " word size=" << data.size() << " to " << path << endl;
    ofstream of(path);
    int data_size = data.size();
    for(int i=0; i<(data_size-1); i++)
    {
        of << data[i];
        of << " ";
    }
    of << data[data_size-1];
    of.close();



//    f = fopen(path, "w");
//    if (f == NULL) {
//        perror("fopen");
//        ret = -1;
//        goto exit;
//    }

//    nwrite = fwrite(buf, sizeof(int), n, f);
//    if (nwrite != n) {
//        perror("fwrite");
//        ret = -1;
//        goto exit;
//    }

exit:
    if (path) {
        free(path);
    }
    return data_size;
}

// Return chunks number of -1 on error
int split(ifstream *f, off_t filesize, char *dir, char *buf, size_t bufsize)
{
    // XXX: we assume that filesize % bufsize == 0
    int chunks = filesize / bufsize;
//    cout << "file size = " << filesize << endl;
//    cout << "buffer size = " << bufsize << endl;
//    cout << "Chunks = " << chunks << endl;
    int size_of_text = 0;
    vector<string> data;
    string word;
//    for (i = 0; i < chunks; i++)
    int i = 0;
    while(!f->eof())
    {
        *f >> word;
//        std::cout << word << endl;
        if(size_of_text+word.size() < bufsize){
            data.push_back(word);
            size_of_text++;
            size_of_text+= word.size();
        }else{
//            cout << "data[" << i << "].size=" << data.size()  << "--> size of text = " << size_of_text<< endl;
            sort(data.begin(), data.end());
//            cout << "sorted: ";
//            copy(data.begin(), data.end(), ostream_iterator<string>(cout, " "));
//            cout << endl << "-------------------------------" << endl;
            save_buf(data, dir, i);

            i++;
            data.clear();
            data.push_back(word);
            size_of_text = word.size();
        }
    }
    return chunks;
}

int merge(char *dir/*, char *buf*//*, size_t bufsize*/, int chunks, size_t offset)
{
    FILE *f;
    int slice;
    char *path;
    int buf_offset;
    int i, n;

//    slice = bufsize / chunks;
    buf_offset = 0;

    // Each chunk has `chunk` number of slices.
    for (i = 0; i < chunks; i++)
    {
        path = form_filename(i, dir);
        fprintf(stderr, "Chunk %s at offset %zu\n", path, offset);

        f = fopen(path, "rb");
        if (f == NULL) {
            perror("fopen");
            return -1;
        }
        fseek(f, offset, SEEK_SET);


        // Accumulate slices from each chunk in buffer.
//        n = fread(buf + buf_offset, 1, slice, f);
        buf_offset += n;
        fclose(f);
    }

    // Thoughout this function we used buf as char array to use byte-addressing.
    // But now we need to act on ints inside that buffer, so we cast it to actual type.
    n = buf_offset / sizeof(int);
//    qsort(buf, n, sizeof(int), compar);
//    print_arr((int *)buf, n);

    return 0;
}
struct lessKMerge{
    bool operator()(pair<ifstream*,string> lhs, pair<ifstream*,string> rhs){
        return  lhs.second.compare(rhs.second)<0;
    }
};


int external_merge_sort(ifstream *f, off_t filesize, char *dir, size_t bufsize)
{
    int chunks;
    char *buf;
    size_t chunk_offset;

    // XXX: Here is the only buffer available to us.
    buf = new char[bufsize];
    if (!buf) {
        perror("new");
        return -ENOMEM;
    }

    // Phase 1: split file to sorted chunks of size bufsize.
    chunks = split(f, filesize, dir, buf, bufsize);
    if (chunks < 0) {
        free(buf);
        return -1;
    }

    // Phase 2: merge chunks.
    chunk_offset = 0;

    multiset<pair<ifstream*, string>, lessKMerge> k_merge;//<chunk_fs,text_value>
    vector<ifstream*> chunk_fs_ptrs;
    for(int i=0; i<chunks; i++){
        ifstream* fs = new ifstream(form_filename(i, dir));
        string word;
        (*fs) >> word;
        chunk_fs_ptrs.push_back(fs);
        pair<ifstream*, string> value(fs,word);
        k_merge.insert(value);
//        cout << "value " << i << ":" << value.second << endl;
    }
    int count=0;
    while(!k_merge.empty()){
//        printf("Sorted: %s\n",k_merge.begin()->second);
        cout << k_merge.begin()->second << endl;
        count++;
        ifstream* fs = k_merge.begin()->first;
        string word;
        if(fs->eof()){
            k_merge.erase(k_merge.begin());
            delete fs;
            continue;
        }
        (*fs) >> word;
        k_merge.erase(k_merge.begin());
        pair<ifstream*, string> temp(fs, word);
        k_merge.insert(temp);
    }
//    cout << "Total: " << count << endl;

//    while (chunk_offset < bufsize)
//    {
//        fprintf(stderr, "-> Merging chunks at offset %zu/%zu\n", chunk_offset, bufsize);
//        merge(dir, buf, bufsize, chunks, chunk_offset);
//        chunk_offset += bufsize / chunks;
//    }

//    free(buf);
    return 0;
}

int main(int argc, const char *argv[])
{
//    FILE *f;

//    cout << "physic pages = " << sysconf(_SC_PHYS_PAGES) << endl
//         << "page size = " << sysconf(_SC_PAGE_SIZE) << endl
//         << "size = " << sysconf(_SC_PHYS_PAGES)*sysconf(_SC_PAGE_SIZE) << endl;

    ifstream *f;
    char *dirpath;
    size_t bufsize;
    char dirname[] = "sort.XXXXXX";
    struct stat sb;
    off_t file_size;
    clock_t start, end;

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <file to sort>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    errno = 0;
    // Check that buffer is aliquot part of file size.
    if (stat(argv[1], &sb)) {
        perror("stat");
        exit(EXIT_FAILURE);
    }

    file_size = sb.st_size;
    bufsize = file_size/16;

//    f = fopen(argv[1], "rb");
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
    fprintf(stderr, "%ld bytes sorted in %f seconds\n", file_size, (double)(end - start) / CLOCKS_PER_SEC);

err:

//    fclose(f);
    f->close();
    return 0;
}
