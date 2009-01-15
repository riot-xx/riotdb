#include <stdio.h>


/* --- Function Prototypes --- */
void template(int *arg1, int *arg2, double *ret);


/* --- Function Definitions --- */
void template(int *arg1, int *arg2, double *ret)
{
   int i;
   
   for( i = 0 ; i < *arg1 ; i++ )
   {
      Rprintf("Hello World!\n");
   }

   *ret = 2.0 * (*arg1) * (*arg2);
   
}


