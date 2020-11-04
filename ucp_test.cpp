#include <cstdio>
#include <cstring>
#include <cassert>

#include <ucp/api/ucp.h>

#include "util.h"

enum test_mode_t {
  TEST_MODE_PROBE,
  TEST_MODE_WAIT,
  TEST_MODE_EVENTFD
};
static test_mode_t test_mode = TEST_MODE_PROBE;

struct my_context {
  int completed;
};

static void request_init(void *request) {
  my_context* ctx = (my_context*)request;
  ctx->completed = 0;
}

static void recv_handler(void *request, ucs_status_t status, ucp_tag_recv_info_t *info) {
  my_context* context = (my_context*)request;
  context->completed = 1;
  printf("Receive handler called with status %d (%s), length %lu\n",
      status, ucs_status_string(status), info->length);
}

static void failure_handler(void *arg, ucp_ep_h ep, ucs_status_t status) {
  ucs_status_t *arg_status = (ucs_status_t*)arg;
  printf("Failure handler called with status %d (%s)\n",
      status, ucs_status_string(status));
  *arg_status = status;
}

static void send_handler(void *request, ucs_status_t status, void *ctx) {
  my_context* context = (my_context*)ctx;
  context->completed = 1;
  printf("Send handler called with status %d (%s)\n",
      status, ucs_status_string(status));
}

int main(int argc, char** argv) {
  /* args setup */
  char* server_name = NULL;
  if (argc == 1) {
    // server
  } else if (argc == 2) {
    // client
    server_name = argv[1];
  } else {
    printf("Usage:\n");
    printf("  server: %s\n", argv[0]);
    printf("  client: %s [server]\n", argv[0]);
    return 0;
  }
  uint16_t server_port = 13337;

  ucs_status_t status;

  /*
   * Setup UCP parameters
   */
  ucp_params_t ucp_params;
  memset(&ucp_params, 0, sizeof(ucp_params));
  ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES
                        | UCP_PARAM_FIELD_REQUEST_SIZE
                        | UCP_PARAM_FIELD_REQUEST_INIT;
  ucp_params.features = UCP_FEATURE_TAG;
  ucp_params.request_size = sizeof(my_context);
  ucp_params.request_init = request_init;

  /*
   * Setup UCP configuration
   */
  ucp_config_t* config;
  status = ucp_config_read(NULL, NULL, &config);
  CHECK_UCS(status);

  /*
   * Create UCP context
   */
  ucp_context_h ucp_context;
  status = ucp_init(&ucp_params, config, &ucp_context);
  CHECK_UCS(status);

  /*
   * Print UCP configuration and release
   */
  ucp_config_print(config, stdout, NULL, UCS_CONFIG_PRINT_CONFIG);
  ucp_config_release(config);

  /*
   * Setup UCP worker parameters
   */
  ucp_worker_params_t worker_params;
  memset(&worker_params, 0, sizeof(worker_params));
  worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;

  /*
   * Create UCP worker
   */
  ucp_worker_h ucp_worker;
  status = ucp_worker_create(ucp_context, &worker_params, &ucp_worker);
  CHECK_UCS(status);

  /*
   * Get address of UCP worker
   */
  ucp_address_t* local_addr;
  size_t local_addr_len;
  status = ucp_worker_get_address(ucp_worker, &local_addr, &local_addr_len);
  CHECK_UCS(status);

  printf("UCP local address(%ldB)=", local_addr_len);
  for (int i = 0; i < local_addr_len; ++i)
    printf(" %02X", ((unsigned char*)local_addr)[i]);
  printf("\n");

  /*
   * Exchange address via out-of-band channel
   */
  int oob_sock;
  ucp_address_t* peer_addr;
  size_t peer_addr_len;
  if (server_name) {
    oob_sock = client_connect(server_name, server_port);
  } else {
    oob_sock = server_connect(server_port);
  }
  CHECK_COND(oob_sock != -1);
  sendrecv(oob_sock, local_addr, local_addr_len, (void**)&peer_addr, &peer_addr_len);

  printf("UCP peer address(%ldB)=", peer_addr_len);
  for (int i = 0; i < peer_addr_len; ++i)
    printf(" %02X", ((unsigned char*)peer_addr)[i]);
  printf("\n");

  const ucp_tag_t tag = 0x1337A880;
  const ucp_tag_t tag_mask = 0xFFFFFFFF;

  size_t msg_len = 1L * 1024 * 1024 * 1024;
  char* msg = (char*)malloc(msg_len);

  if (server_name) {
    /*
     * UCP client
     */

    /*
     * Create ep to server
     */
    ucp_ep_params_t ep_params;
    ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
    ep_params.address = peer_addr;

    ucp_ep_h server_ep;
    status = ucp_ep_create(ucp_worker, &ep_params, &server_ep);
    CHECK_UCS(status);

    ucp_tag_message_h msg_tag;
    ucp_tag_recv_info_t info_tag;
    my_context* request;
    double st, et;

    for (int i = 0; ; ++i) {
      st = GetTime();

      /*
       * Probe message to receive
       */
      while (true) {
        msg_tag = ucp_tag_probe_nb(ucp_worker, tag, tag_mask, 1, &info_tag);
        if (msg_tag != NULL) break;
        ucp_worker_progress(ucp_worker);
      }

      /*
       * Post non-blocking receive
       */
      request = (my_context*)ucp_tag_msg_recv_nb(ucp_worker, msg, info_tag.length, ucp_dt_make_contig(1), msg_tag, recv_handler);
      if (UCS_PTR_IS_ERR(request)) {
        printf("UCP receive failed. (%u)\n", UCS_PTR_STATUS(request));
        exit(EXIT_FAILURE);
      } else if (UCS_PTR_IS_PTR(request)) {
        printf("Polling UCP recv completion...\n");
        while (request->completed == 0) {
          ucp_worker_progress(ucp_worker);
        }
        request->completed = 0;
        ucp_request_free(request);
      } else {
        assert(false && "Should not reach here");
      }

      et = GetTime();
      printf("[%d] %f s, %f GB/s\n", i, et - st, msg_len / 1e9 / (et - st));
    }
  } else {
    /*
     * UCP server
     */

    /*
     * Create ep to client
     */
    ucp_ep_params_t ep_params;
    ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
    ep_params.address = peer_addr;

    ucp_ep_h client_ep;
    status = ucp_ep_create(ucp_worker, &ep_params, &client_ep);
    CHECK_UCS(status);

    my_context ctx;
    ucp_request_param_t send_param;
    ucs_status_ptr_t status;
    double st, et;

    ctx.completed = 0;

    send_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK
                            | UCP_OP_ATTR_FIELD_USER_DATA;
    send_param.cb.send = send_handler;
    send_param.user_data = &ctx; // passed to send_handler

    for (int i = 0; ; ++i) {
      st = GetTime();

      /*
       * Post non-blocking send
       */
      status = ucp_tag_send_nbx(client_ep, msg, msg_len, tag, &send_param);
      if (UCS_PTR_IS_ERR(status)) {
        printf("UCP send failed. (%u)\n", UCS_PTR_STATUS(status));
        exit(EXIT_FAILURE);
      } else if ((long)status == UCS_OK) {
        printf("UCP sent immediately. Callback will not be called.\n");
      } else if (UCS_PTR_IS_PTR(status)) {
        printf("Polling UCP send completion...\n");
        while (ctx.completed == 0) {
          ucp_worker_progress(ucp_worker);
        }
        ctx.completed = 0;
        ucp_request_free(status);
      } else {
        assert(false && "Should not reach here");
      }

      et = GetTime();
      printf("[%d] %f s, %f GB/s\n", i, et - st, msg_len / 1e9 / (et - st));
    }
  }

  return 0;
}
