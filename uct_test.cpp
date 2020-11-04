#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cassert>

#include <uct/api/uct.h>

#include "util.h"

enum func_am_t {
  FUNC_AM_SHORT,
  FUNC_AM_BCOPY,
  FUNC_AM_ZCOPY
};

struct recv_desc_t {
  int is_uct_desc;
};

struct bcopy_args {
  char               *data;
  size_t              len;
};

struct zcopy_args {
  uct_completion_t    uct_comp;
  uct_md_h            md;
  uct_mem_h           memh;
};

static void* desc_holder = NULL;

static ucs_status_t am_handler(void *arg, void *data, size_t length, unsigned flags) {
  printf("Active message handler called! (len=%ld)\n", length);

  func_am_t func_am_type = *(func_am_t *)arg;
  recv_desc_t *rdesc;

  if (flags & UCT_CB_PARAM_FLAG_DESC) {
    printf("flag has UCT_CB_PARAM_FLAG_DESC.\n");
    printf("We own the buffer, which should be released later.\n");
    rdesc = (recv_desc_t *)data - 1;
    rdesc->is_uct_desc = 1;
    desc_holder = rdesc;
    return UCS_INPROGRESS;
  }

  printf("flag does not have UCT_CB_PARAM_FLAG_DESC.\n");
  printf("We allocate a new buffer and copy out the data.\n");
  rdesc = (recv_desc_t*)malloc(sizeof(*rdesc) + length);
  rdesc->is_uct_desc = 0;
  memcpy(rdesc + 1, data, length);
  desc_holder = rdesc;
  return UCS_OK;
}

size_t bcopy_packer(void *dest, void *arg) {
  bcopy_args *bc_args = (bcopy_args*)arg;
  memcpy(dest, bc_args->data, bc_args->len);
  return bc_args->len;
}

void zcopy_completion_cb(uct_completion_t *self, ucs_status_t status) {
  zcopy_args *comp = (zcopy_args*)self;
  assert((comp->uct_comp.count == 0) && (status == UCS_OK));
  if (comp->memh != UCT_MEM_HANDLE_NULL) {
    uct_md_mem_dereg(comp->md, comp->memh);
  }
  desc_holder = (void *)0xDEADBEEF;
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
  func_am_t func_am_type = FUNC_AM_ZCOPY;
  long test_strlen = 8;
  //const char* dev_name = "mlx5_0:1";
  //const char* tl_name = "rc_mlx5";
  const char* dev_name = "ibs6";
  const char* tl_name = "tcp";
  ucs_memory_type_t test_mem_type = UCS_MEMORY_TYPE_HOST; // HOST / CUDA / CUDA_MANAGED

  ucs_status_t status;

  /*
   * ucs context creation
   * Better to use different contexts for different workers, according to hello_world
   */
  ucs_async_context_t* async;
  status = ucs_async_context_create(UCS_ASYNC_MODE_THREAD_SPINLOCK, &async);
  CHECK_UCS(status);

  /*
   * uct worker creation
   */
  uct_worker_h worker;
  status = uct_worker_create(async, UCS_THREAD_MODE_SINGLE, &worker);
  CHECK_UCS(status);

  /*
   * Enumerate uct components, memory domains, and communication resources.
   * Open an interface with matching device name and transport name.
   */
  uct_component_h* components;
  unsigned num_components;
  status = uct_query_components(&components, &num_components);
  CHECK_UCS(status);

  uct_md_h md;
  uct_md_attr_t md_attr;
  uct_iface_h iface;
  uct_iface_attr_t iface_attr;
  for (int i = 0; i < num_components; ++i) {
    uct_component_attr_t component_attr;
    component_attr.field_mask = UCT_COMPONENT_ATTR_FIELD_NAME
      | UCT_COMPONENT_ATTR_FIELD_MD_RESOURCE_COUNT;
    status = uct_component_query(components[i], &component_attr);
    CHECK_UCS(status);
    printf("uct_comp[%d]: %s\n", i, component_attr.name);

    component_attr.field_mask = UCT_COMPONENT_ATTR_FIELD_MD_RESOURCES;
    component_attr.md_resources = (uct_md_resource_desc_t*)malloc(component_attr.md_resource_count * sizeof(uct_md_resource_desc_t));
    status = uct_component_query(components[i], &component_attr);
    CHECK_UCS(status);

    for (int j = 0; j < component_attr.md_resource_count; ++j) {
      uct_md_config_t* md_config;
      status = uct_md_config_read(components[i], NULL, NULL, &md_config);
      CHECK_UCS(status);

      status = uct_md_open(components[i], component_attr.md_resources[j].md_name, md_config, &md);
      uct_config_release(md_config);
      CHECK_UCS(status);

      status = uct_md_query(md, &md_attr);
      CHECK_UCS(status);

      uct_tl_resource_desc_t* tl_resources;
      unsigned num_tl_resources;
      status = uct_md_query_tl_resources(md, &tl_resources, &num_tl_resources);
      CHECK_UCS(status);
      printf("  md[%d]: %s\n", j, component_attr.md_resources[j].md_name);

      for (int k = 0; k < num_tl_resources; ++k) {
        printf("    tl[%d]: %s/%s\n", k, tl_resources[k].tl_name, tl_resources[k].dev_name);

        if (!strcmp(tl_resources[k].tl_name, tl_name) && !strcmp(tl_resources[k].dev_name, dev_name)) {
          printf("    ^^^^ OPENED ^^^^\n");
          uct_iface_params_t params;
          params.field_mask           = UCT_IFACE_PARAM_FIELD_OPEN_MODE
            | UCT_IFACE_PARAM_FIELD_DEVICE
            | UCT_IFACE_PARAM_FIELD_STATS_ROOT
            | UCT_IFACE_PARAM_FIELD_RX_HEADROOM
            | UCT_IFACE_PARAM_FIELD_CPU_MASK;
          params.open_mode            = UCT_IFACE_OPEN_MODE_DEVICE;
          params.mode.device.tl_name  = tl_resources[k].tl_name;
          params.mode.device.dev_name = tl_resources[k].dev_name;
          params.stats_root           = NULL;
          params.rx_headroom          = sizeof(recv_desc_t);
          UCS_CPU_ZERO(&params.cpu_mask);

          uct_iface_config_t* config;
          status = uct_md_iface_config_read(md, tl_resources[k].tl_name, NULL, NULL, &config);
          CHECK_UCS(status);

          status = uct_iface_open(md, worker, &params, config, &iface);
          uct_config_release(config);
          CHECK_UCS(status);

          uct_iface_progress_enable(iface, UCT_PROGRESS_SEND | UCT_PROGRESS_RECV);

          status = uct_iface_query(iface, &iface_attr);
          CHECK_UCS(status);

          uct_release_tl_resource_list(tl_resources);
          free(component_attr.md_resources);
          uct_release_component_list(components);

          goto iface_opened;
        }
      }
      uct_release_tl_resource_list(tl_resources);
      uct_md_close(md);
    }
    free(component_attr.md_resources);
  }
  uct_release_component_list(components);

  printf("Transport not found.\n");
  exit(EXIT_FAILURE);

iface_opened:

  /*
   * Open out-of-band connection
   */
  int oob_sock;
  if (server_name) {
    oob_sock = client_connect(server_name, server_port);
  } else {
    oob_sock = server_connect(server_port);
  }

  /*
   * Exchange device address.
   */
  uct_device_addr_t* own_dev;
  own_dev = (uct_device_addr_t*)calloc(1, iface_attr.device_addr_len);

  status = uct_iface_get_device_address(iface, own_dev);
  CHECK_UCS(status);

  printf("own_dev =");
  for (int i = 0; i < iface_attr.device_addr_len; ++i) printf(" %02X", ((unsigned char*)own_dev)[i]);
  printf("\n");

  uct_device_addr_t* peer_dev;
  sendrecv(oob_sock, own_dev, iface_attr.device_addr_len, (void **)&peer_dev);

  /*
   * Exchange interface address.
   */
  uct_iface_is_reachable(iface, peer_dev, NULL);

  uct_iface_addr_t* peer_iface;
  if (iface_attr.cap.flags & UCT_IFACE_FLAG_CONNECT_TO_IFACE) {
    printf("Exchanging interface address...\n");

    uct_iface_addr_t* own_iface;
    own_iface = (uct_iface_addr_t*)calloc(1, iface_attr.iface_addr_len);

    status = uct_iface_get_address(iface, own_iface);
    CHECK_UCS(status);

    printf("own_iface =");
    for (int i = 0; i < iface_attr.iface_addr_len; ++i) printf(" %02X", ((unsigned char*)own_iface)[i]);
    printf("\n");

    sendrecv(oob_sock, own_iface, iface_attr.iface_addr_len, (void **)&peer_iface);
  } else {
    printf("Skipping interface address exchange...\n");
  }

  /*
   * Exchange endpoint address.
   */
  uct_ep_params_t     ep_params;
  ep_params.field_mask = UCT_EP_PARAM_FIELD_IFACE;
  ep_params.iface      = iface;
  uct_ep_h ep;
  if (iface_attr.cap.flags & UCT_IFACE_FLAG_CONNECT_TO_EP) {
    printf("Exchanging endpoint address...\n");

    uct_ep_addr_t* own_ep;
    own_ep = (uct_ep_addr_t*)calloc(1, iface_attr.ep_addr_len);

    status = uct_ep_create(&ep_params, &ep);
    CHECK_UCS(status);

    status = uct_ep_get_address(ep, own_ep);
    CHECK_UCS(status);

    printf("own_ep =");
    for (int i = 0; i < iface_attr.ep_addr_len; ++i) printf(" %02X", ((unsigned char*)own_ep)[i]);
    printf("\n");

    uct_ep_addr_t* peer_ep;
    sendrecv(oob_sock, own_ep, iface_attr.ep_addr_len, (void **)&peer_ep);

    status = uct_ep_connect_to_ep(ep, peer_dev, peer_ep);

    barrier(oob_sock);
  } else {
    assert(iface_attr.cap.flags & UCT_IFACE_FLAG_CONNECT_TO_IFACE);

    printf("Creating endpoint...\n");

    ep_params.field_mask |= UCT_EP_PARAM_FIELD_DEV_ADDR
      | UCT_EP_PARAM_FIELD_IFACE_ADDR;
    ep_params.dev_addr    = peer_dev;
    ep_params.iface_addr  = peer_iface;
    status = uct_ep_create(&ep_params, &ep);
    CHECK_UCS(status);
  }

  printf("max_short = %ld\n", iface_attr.cap.am.max_short);
  printf("max_bcopy = %ld\n", iface_attr.cap.am.max_bcopy);
  printf("max_zcopy = %ld\n", iface_attr.cap.am.max_zcopy);

  uint8_t id = 0;
  status = uct_iface_set_am_handler(iface, id, am_handler, &func_am_type, 0);
  CHECK_UCS(status);

  if (server_name) {
    size_t bufsz = test_strlen;
    char* buf = (char*)malloc(bufsz);

    if (func_am_type == FUNC_AM_SHORT) {
      printf("Send with short...\n");
      uint64_t header = *(uint64_t*)buf;
      char* payload;
      size_t len;
      if (len > sizeof(header)) {
        payload = buf + sizeof(header);
        len = bufsz - sizeof(header);
      } else {
        payload = NULL;
        len = 0;
      }
      do {
        /*
         * For short, we need header + payload + length.
         * ucs_status_t will be returned
         */
        status = uct_ep_am_short(ep, id, header, payload, len);
        uct_worker_progress(worker);
      } while (status == UCS_ERR_NO_RESOURCE);
      CHECK_UCS(status);
    } else if (func_am_type == FUNC_AM_BCOPY) {
      printf("Send with bcopy...\n");
      ssize_t len;
      bcopy_args args;
      args.data = buf;
      args.len = bufsz;
      do {
        /*
         * For bcopy, we need packer callback + argument pointer.
         * Sent length or error code will be returned.
         */
        len = uct_ep_am_bcopy(ep, id, bcopy_packer, &args, 0);
        uct_worker_progress(worker);
      } while (len == UCS_ERR_NO_RESOURCE);
      CHECK_UCS(len >= 0 ? UCS_OK : (ucs_status_t)len);
    } else if (func_am_type == FUNC_AM_ZCOPY) {
      printf("Send with zcopy...\n");

      uct_mem_h memh;
      if (md_attr.cap.flags & UCT_MD_FLAG_NEED_MEMH) {
        printf("Need memory handle. Registering memory...\n");
        status = uct_md_mem_reg(md, buf, bufsz, UCT_MD_MEM_ACCESS_RMA, &memh);
        CHECK_UCS(status);
      } else {
        printf("Do not need memory handle.\n");
        memh = UCT_MEM_HANDLE_NULL;
      }

      uct_iov_t iov;
      iov.buffer          = buf;
      iov.length          = bufsz;
      iov.memh            = memh;
      iov.stride          = 0;
      iov.count           = 1;

      zcopy_args comp;
      comp.uct_comp.func  = zcopy_completion_cb;
      comp.uct_comp.count = 1;
      comp.md             = md;
      comp.memh           = memh;

      do {
        /*
         * For zcopy, pass iov + completion callback.
         * Worker may return UCS_INPROGRESS.
         */
        status = uct_ep_am_zcopy(ep, id, NULL, 0, &iov, 1, 0, (uct_completion_t*)&comp);
        uct_worker_progress(worker);
      } while (status == UCS_ERR_NO_RESOURCE);

      if (status == UCS_INPROGRESS) {
        printf("UCS_INPROGRESS returned. Forcing worker to progress...\n");
        while (!desc_holder) {
          uct_worker_progress(worker);
        }
        status = UCS_OK;
      }
      CHECK_UCS(status);
    } else {
      assert(false && "Unsupported type");
    }
    free(buf);
  } else {
    recv_desc_t *rdesc;

    while (desc_holder == NULL) {
      uct_worker_progress(worker);
    }

    rdesc = (recv_desc_t*)desc_holder;

    if (rdesc->is_uct_desc) {
      uct_iface_release_desc(rdesc);
    } else {
      free(rdesc);
    }
  }

  barrier(oob_sock);

  uct_ep_destroy(ep);
  uct_iface_close(iface);
  uct_md_close(md);
  uct_worker_destroy(worker);
  ucs_async_context_destroy(async);

  return 0;
}
