#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

int main(int argc, const char *argv[])
{
	unsigned int i = 0, gen_file_size = 0;
	unsigned int seed = 0x16372789;
	long int r = 0;
	int is_binary = 0;

	if (argc < 2)
	{
		fprintf(stderr, "Usage: %s <size of file (in bytes)> [random seed]\n", argv[0]);
		//fprintf(stderr, "with -b option output will be binary\n");
		return EXIT_FAILURE;
	}

	if (argc > 2)
	{
		if (!strncmp(argv[1], "-b", 3))
		{
			is_binary = 1;

			// Pretend we didn't have that option ;-)
			argv++;
			argc--;
		}
	}

	errno = 0;
	gen_file_size = strtol(argv[1], NULL, 0);
	if (errno)
	{
		perror("strtol");
		return EXIT_FAILURE;
	}

	if (argc == 3)
	{
		errno = 0;
		seed = strtol(argv[2], NULL, 0);
		if (errno)
		{
			perror("strtol");
			return EXIT_FAILURE;
		}
	}

	if (is_binary)
		freopen(NULL, "wb", stdout);

	int total_size_so_far = 0;
	int wordsize = 0;
	char letter;

	srandom(seed);

	while(total_size_so_far < gen_file_size)
	{
		wordsize = 1 + random()%14; // random word length
		while(total_size_so_far + wordsize > gen_file_size)
			wordsize = 1 + random()%14; 
		for(int j = 0; j < wordsize; j++)
		{
			letter = 'a' + random()%26;
			if(is_binary)
				fwrite(&letter, sizeof(letter), 1, stdout);
			else
				printf("%c", letter);
		}
		total_size_so_far += wordsize;
		if(total_size_so_far + 1 < gen_file_size)
		{	
			letter = ' ';
			if(is_binary)
				fwrite(&letter, sizeof(letter), 1, stdout);
			else
				printf(" ");
			 
			total_size_so_far++;
		}
	}
	return 0;
}

