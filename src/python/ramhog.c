/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <unistd.h>

/*
 * Hogs the specified amount of memory for benchmarking purposes
 */
int 
main(int argc, char *argv[]) {
  int pagesize;
  size_t megabytes, bytes;
  struct rlimit ulimit;
  void *addr;

  if (argc != 2) {
    printf("usage: %s <megabytes>\n", argv[0]);
    return 1;
  }

  pagesize = getpagesize();
  megabytes = atoi(argv[1]);
  bytes = ((megabytes * 1024 * 1024) / pagesize) * pagesize;

  ulimit.rlim_cur = ulimit.rlim_max = RLIM_INFINITY;
  if (setrlimit(RLIMIT_AS, &ulimit) != 0) {
    if (getrlimit(RLIMIT_AS, &ulimit) != 0) {
      perror("couldn't retrieve address space ulimit");
      return 1;
    }

    if (ulimit.rlim_cur != RLIM_INFINITY && ulimit.rlim_cur < bytes) {
      printf("please fix ulimit -v: soft=%llu, hard=%llu\n", 
             (unsigned long long)ulimit.rlim_cur, (unsigned long long)ulimit.rlim_max);
      return 1;
    }
  }

  ulimit.rlim_cur = ulimit.rlim_max = RLIM_INFINITY;
  if (setrlimit(RLIMIT_MEMLOCK, &ulimit) != 0) {

    if (getrlimit(RLIMIT_MEMLOCK, &ulimit) != 0) {
      perror("couldn't retrieve mlock ulimit");
      return 1;
    }

    if (ulimit.rlim_cur != RLIM_INFINITY && ulimit.rlim_cur < bytes) {
      printf("please fix ulimit -l: soft=%llu, hard=%llu\n", 
            (unsigned long long)ulimit.rlim_cur, (unsigned long long)ulimit.rlim_max);
      return 1;
    }
  }

  printf("%s: attempting to hog %lu bytes...\n", argv[0], bytes);

  addr = mmap(0, bytes, PROT_WRITE, MAP_PRIVATE|MAP_ANON, -1, 0);
  if (addr == MAP_FAILED) {
    perror("couldn't map memory");
    return 1;
  }

  if (mlock(addr, bytes) != 0) {
    perror("couldn't mlock memory");
    return 1;
  }

  printf("%s: hogging %lu bytes: sleeping...\n", argv[0], bytes);

  while (1) {
    sleep(100);
  }

  return 0;
}
