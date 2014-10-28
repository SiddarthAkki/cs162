#include "userprog/syscall.h"
#include <stdio.h>
#include <syscall-nr.h>
#include "threads/interrupt.h"
#include "threads/thread.h"

static void syscall_handler (struct intr_frame *);

void
syscall_init (void) 
{
  intr_register_int (0x30, 3, INTR_ON, syscall_handler, "syscall");
}

static void
syscall_handler (struct intr_frame *f UNUSED) 
{
  uint32_t* args = ((uint32_t*) f->esp);
  //printf("System call number: %d\n", args[0]);
  char* str;
  switch(args[0]) 
  {
  	case SYS_EXIT:
      wait_status *cur = thread_current()->parent_wait;
      if (cur != NULL) {
        cur->exit_code = args[1];
      }
     	thread_exit();
  		break;
  	case SYS_NULL:
  		f->eax = args[1]+1;
  		break;
  	case SYS_WRITE:
	        
  		printf("%s\n", ((char*) args[2]));
  		f->eax = args[3];
  		break;

  	default:
  		break;
  }
}




    // SYS_HALT,                   /* Halt the operating system. */
    // SYS_EXIT,                   /* Terminate this process. */
    // SYS_EXEC,                   /* Start another process. */
    // SYS_WAIT,                   /* Wait for a child process to die. */
    // SYS_CREATE,                 /* Create a file. */
    // SYS_REMOVE,                 /* Delete a file. */
    // SYS_OPEN,                   /* Open a file. */
    // SYS_FILESIZE,               /* Obtain a file's size. */
    // SYS_READ,                   /* Read from a file. */
    // SYS_WRITE,                  /* Write to a file. */
    // SYS_SEEK,                   /* Change position in a file. */
    // SYS_TELL,                   /* Report current position in a file. */
    // SYS_CLOSE,                  /* Close a file. */
    // SYS_NULL,                   /* Returns arg incremented by 1 */
