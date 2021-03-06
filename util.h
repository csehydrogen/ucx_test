#pragma once

#include <cstdlib>
#include <ctime>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <netdb.h>

#define CHECK_UCS(status) \
  do { \
    ucs_status_t CHECK_UCS_status = (status); \
    if (CHECK_UCS_status != UCS_OK) { \
      printf("[%s:%d] ucs error (status=%d)\n", __FILE__, __LINE__, CHECK_UCS_status); \
      exit(EXIT_FAILURE); \
    } \
  } while (false)

#define CHECK_COND(cond) \
  do { \
    bool CHECK_COND_cond = (cond); \
    if (!CHECK_COND_cond) { \
      printf("[%s:%d] false condition\n", __FILE__, __LINE__); \
      exit(EXIT_FAILURE); \
    } \
  } while (false)

static double GetTime() {
  timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec + t.tv_nsec / 1e9;
}

static int server_connect(uint16_t server_port) {
  struct sockaddr_in inaddr;
  int lsock, dsock, optval, ret;

  lsock = socket(AF_INET, SOCK_STREAM, 0);
  CHECK_COND(lsock >= 0);

  optval = 1;
  ret = setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
  CHECK_COND(ret >= 0);

  inaddr.sin_family      = AF_INET;
  inaddr.sin_port        = htons(server_port);
  inaddr.sin_addr.s_addr = INADDR_ANY;
  memset(inaddr.sin_zero, 0, sizeof(inaddr.sin_zero));
  ret = bind(lsock, (struct sockaddr*)&inaddr, sizeof(inaddr));
  CHECK_COND(ret >= 0);

  ret = listen(lsock, 0);
  CHECK_COND(ret >= 0);

  dsock = accept(lsock, NULL, NULL);
  CHECK_COND(dsock >= 0);

  close(lsock);

  return dsock;
}

static int client_connect(const char *server, uint16_t server_port) {
  struct sockaddr_in conn_addr;
  struct hostent *he;
  int connfd, ret;

  connfd = socket(AF_INET, SOCK_STREAM, 0);
  CHECK_COND(connfd >= 0);

  he = gethostbyname(server);
  CHECK_COND(he != NULL && he->h_addr_list != NULL);

  conn_addr.sin_family = he->h_addrtype;
  conn_addr.sin_port   = htons(server_port);

  memcpy(&conn_addr.sin_addr, he->h_addr_list[0], he->h_length);
  memset(conn_addr.sin_zero, 0, sizeof(conn_addr.sin_zero));

  ret = connect(connfd, (struct sockaddr*)&conn_addr, sizeof(conn_addr));
  CHECK_COND(ret >= 0);

  return connfd;
}

static int sendrecv(int sock, const void *sbuf, size_t slen, void **rbuf, size_t* rlen) {
  int ret = 0;
  size_t rlen_ = 0;
  *rbuf = NULL;

  ret = send(sock, &slen, sizeof(slen), 0);
  if ((ret < 0) || (ret != sizeof(slen))) {
    fprintf(stderr, "failed to send buffer length\n");
    return -1;
  }

  ret = send(sock, sbuf, slen, 0);
  if (ret != (int)slen) {
    fprintf(stderr, "failed to send buffer, return value %d\n", ret);
    return -1;
  }

  ret = recv(sock, &rlen_, sizeof(rlen_), MSG_WAITALL);
  if ((ret != sizeof(rlen_)) || (rlen_ > (SIZE_MAX / 2))) {
    fprintf(stderr, "failed to receive device address length, return value %d\n", ret);
    return -1;
  }
  if (rlen) *rlen = rlen_;

  *rbuf = calloc(1, rlen_);
  if (!*rbuf) {
    fprintf(stderr, "failed to allocate receive buffer\n");
    return -1;
  }

  ret = recv(sock, *rbuf, rlen_, MSG_WAITALL);
  if (ret != (int)rlen_) {
    fprintf(stderr, "failed to receive device address, return value %d\n", ret);
    return -1;
  }

  return 0;
}

static int sendrecv(int sock, const void *sbuf, size_t slen, void **rbuf) {
  return sendrecv(sock, sbuf, slen, rbuf, NULL);
}

static int barrier(int oob_sock) {
  int dummy = 0;
  ssize_t res;

  res = send(oob_sock, &dummy, sizeof(dummy), 0);
  if (res < 0) {
    return res;
  }

  res = recv(oob_sock, &dummy, sizeof(dummy), MSG_WAITALL);

  return !(res == sizeof(dummy));
}

static void print_addrinfo(addrinfo* res) {
  for (addrinfo* it = res; it != NULL; it = it->ai_next) {
    char host[99], serv[99];
    int ret = getnameinfo(it->ai_addr, it->ai_addrlen, host, sizeof(host), serv, sizeof(serv), NI_NUMERICHOST | NI_NUMERICSERV);
    printf("host=%s, serv=%s, flags=%d, family=%d, socktype=%d, protocol=%d, addrlen=%d, err=%d(%s)\n", host, serv, it->ai_flags, it->ai_family, it->ai_socktype, it->ai_protocol, it->ai_addrlen, ret, ret == 0 ? "No error" : gai_strerror(ret));
  }
}
