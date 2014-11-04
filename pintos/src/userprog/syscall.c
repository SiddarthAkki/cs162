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
#include "filesys/filesys.h"

static void syscall_handler (struct intr_frame *);
void find_next_fd(struct thread *curr);
int find_fd(struct thread *curr_thread, uint32_t* args);
void read_stdin(void *dst, size_t size);

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
  struct thread *curr_thread = thread_current();
  switch(args[0]) 
  {
    case SYS_HALT:
        shutdown_power_off();
        break;

    case SYS_EXIT:
        cur_status = thread_current()->parent_wait;
        if (cur_status != NULL && valid_pointer(args + 1)) {
          cur_status->exit_code = args[1];
        }
        thread_exit();
        break;
    //open file and call file_deny_write()
    //when process exits call file_allow_write()
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

    case SYS_CREATE:
        if (valid_pointer(args[1])) {
          f->eax = filesys_create(args[1], args[2]);
        } else {
          thread_exit();
        }
        break;

    case SYS_REMOVE:
        if (valid_pointer(args+1) && valid_pointer(args[1])) {
          bool removed = filesys_remove(args[1]);
          f->eax = removed;
        } else {
          thread_exit();
        }
        break;

    case SYS_OPEN:
        if (valid_pointer(args+1) && valid_pointer(args[1])) {
          if (curr_thread->fd_curr < 128) {
            f->eax = find_fd(curr_thread, args);
          } else {
            f->eax = -1;
          }
        } else {
          thread_exit();
        }
        break;

    case SYS_FILESIZE:
        if (((curr_thread->fd_table)[args[1]]) != NULL) {
          f->eax = file_length((curr_thread->fd_table)[args[1]]);
        } else {
          thread_exit();
        }
        break;

    case SYS_READ:
        if (valid_pointer(args+2) && valid_pointer(args[2])) {
          if (args[1] == 0) {
            read_stdin(args[2], args[3]);
            f->eax = args[3];
          } else {
            if (args[1] < 128 && args[1] > 0) {
              if (((curr_thread->fd_table)[args[1]]) != NULL) {
                f->eax = file_read(((curr_thread->fd_table)[args[1]]), args[2], args[3]);
              } else {
                f->eax = -1;
              }
            } else {
              f->eax = -1;
            }
          }
        } else {
          thread_exit();
        }
        break;

    case SYS_NULL:
        f->eax = args[1]+1;
        break;

    case SYS_WRITE:
        if (valid_pointer(args[2]) && valid_pointer(args[2])) {
          if (args[1] == 1) {
            putbuf(args[2], args[3]);
          } else {
            if (args[1] < 128 && args[1] > 0) {
              if (((curr_thread->fd_table)[args[1]]) != NULL) {
                f->eax = file_write(((curr_thread->fd_table)[args[1]]), args[2], args[3]);
              } else {
                f->eax = -1;
              }
            } else {
              f->eax = -1;
            }
          }
        } else {
          thread_exit();
        }
        break;

    case SYS_SEEK:
        break;

    case SYS_TELL:
        break;

    default:
        break;
  }
}

void read_stdin(void *dst, size_t size) {
  int i;
  char c;
  for (i = 0; i < size; i++) {
    c = input_getc();
    memcpy(dst, (void *)&c, 1);
    dst++;
  }
}

int find_fd(struct thread *curr_thread, uint32_t* args) {
  struct file *curr_file = filesys_open(args[1]);
  int fd;
  if (curr_file != NULL) {
    fd = curr_thread->fd_curr;
    curr_thread->fd_table[fd] = curr_file;
    find_next_fd(curr_thread);
  } else {
    fd = -1;
  }
  return fd;
}

void find_next_fd(struct thread *curr) {
  uint32_t i = (curr->fd_curr)+1;

  for (i = (curr->fd_curr)+1; i < 128; i++) {
    if (curr->fd_table[i] == NULL) {
      curr->fd_curr = i;
      break;
    }
  }
  if (i == 128) {
    curr->fd_curr = 128;
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
