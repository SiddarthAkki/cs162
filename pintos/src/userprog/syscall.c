#include "userprog/syscall.h"
#include <stdio.h>
#include <syscall-nr.h>
#include "threads/interrupt.h"
#include "threads/thread.h"
#include "userprog/process.h"
#include "threads/init.h"
#include <stdbool.h>
#include "userprog/pagedir.h"
#include "threads/vaddr.h"

static void syscall_handler (struct intr_frame *);

void
syscall_init (void) 
{
  intr_register_int (0x30, 3, INTR_ON, syscall_handler, "syscall");
}

static int valid_pointer (void *vaddr) {
  struct thread *cur = thread_current ();
  uint32_t *pd;
  pd = cur->pagedir;

  return is_user_vaddr(vaddr) && (NULL != pagedir_get_page(pd, vaddr));
}

static void
syscall_handler (struct intr_frame *f UNUSED) 
{
  uint32_t* args = ((uint32_t*) f->esp);
  //printf("System call number: %d\n", args[0]);
  char* str;
  struct wait_status *cur_status;
  if (!valid_pointer(args))
  {
      thread_exit();
  }
  switch(args[0]) 
  {
    case SYS_HALT:
        shutdown_power_off();
        break;

    case SYS_EXIT:
        cur_status = thread_current()->parent_wait;
        if (cur_status != NULL) {
          cur_status->exit_code = args[1];
        }
        thread_exit();
        break;

    case SYS_EXEC:
        if (valid_pointer(args[1])){
          f->eax = process_execute(args[1]);
        } else {
          thread_exit();
        }
        break;

    case SYS_WAIT:
        f->eax = process_wait(args[1]);
        break;

    case SYS_NULL:
        f->eax = args[1]+1;
        break;

    case SYS_WRITE:
        if (valid_pointer(args[2])){
          printf("%s", ((char*) args[2]));
          f->eax = args[3];
        } else {
          thread_exit();
        }
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
