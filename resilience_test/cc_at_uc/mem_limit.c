// a simple c-program that allocates 1MB in each
// iteration and run for a total of 50 iterations,
// allocating a total of 50 MB
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(void)
{
    int i;
    char *p;
    for (i = 0; i < 50000000000; ++i)
    {
        // Allocate 1 MB each time.
        if ((p = malloc(1<<20)) == NULL)
        {
            printf("Malloc failed at %d MB\n", i);
            return 0;
        }
        memset(p, 0, (1<<20));
        printf("Allocated %d to %d MB\n", i, i+1);
    }

    printf("Done!\n");
    return 0;
}