/* Tests the null syscall */

#include "tests/lib.h"
#include "tests/main.h"
#include <string.h>

void
test_main (void) 
{
  char *msg = "I love CS162";
  write(0, msg, strlen(msg));
}
