# ucx_test

## UCT

UCT is a transport layer. UCT provides context management, and allocation of device-specific memories. UCT provides three communication APIs: immediate (short), buffered copy-and-send (bcopy), and zero-copy (zcopy).

* Short: For small messages that can be posted and completed in place.
* Bcopy: For medium-sized messages that are typically sent through a so-called bouncing buffer. The auxiliary buffer is typically allocated and ready for immediate utilization by the hardware. Non-contiguous I/O is supported through a custom data packing callback.
* Zcopy: Exposes zero-copy memory-to-memory communication semantics.

### Program Flow

* Create UCS context(`ucs_async_context_t`)
* Create UCT worker(`uct_worker_h`)
* Enumerate UCT components(`uct_component_h`), memory domains(`uct_md_h`), and transports(`uct_tl_resource_desc_t`). Open an interface(`uct_iface_h`) you want.
* Open an out-of-band (OOB) connection to exchange device address(`uct_device_addr_t`), interface address(`uct_iface_addr_t`), and endpoint address(`uct_ep_addr_t`).
* Open an endpoint(`uct_ep_h`) based on exchanged information.
  * For some transports(e.g., InfiniBand), interface address does not exist and EP will be connected with endpoint address.
  * For others(e.g., TCP), enpoint address does not exist and EP will be created with interface address.
* Set an active message handler.
* Server: send data with short, bcopy, or zcopy.
  * Short requires only data.
  * Bcopy requires data and packer callback.
  * Zcopy requires memory registering(if `UCT_MD_FLAG_NEED_MEMH`), description of buffer to send(`uct_iov_t`), and completion callback.
* Receiver: receive data through a registered active message handler.
