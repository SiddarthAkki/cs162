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

  switch(args[0]) 
  {
  	case SYS_EXIT:
  		f->eax = args[1];
     	thread_exit();
  		break;
  	case SYS_NULL:
  		printf("\n%d\n", args[1]);
  		f->eax = args[1]+1;
  		printf("\n%d\n", f->eax);
  		break;
  	case SYS_WRITE:
  		break;

  	default:
  		printf("hi\n");
  		break;
  }

  // if (args[0] == SYS_EXIT) {
  //   f->eax = args[1];
  //   printf("exit code: %d\n", args[1]);
  //   thread_exit();
  // }
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