#ifndef USERPROG_PROCESS_H
#define USERPROG_PROCESS_H

#include "threads/thread.h"
#include "threads/synch.h"

tid_t process_execute (const char *file_name);
int process_wait (tid_t);
void process_exit (void);
void process_activate (void);

typedef struct wait_status {
  struct list_elem elem;
  struct lock lock;
  int ref_cnt;
  tid_t tid;
  int exit_code;
  struct semaphore dead;
  int initial_success;
  struct semaphore success;
} wait_status;

#endif /* userprog/process.h */
