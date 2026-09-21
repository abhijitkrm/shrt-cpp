#pragma once
#include "store.hpp"

namespace shrt {

/// Blocking accept loop, one thread per connection (SERVER=mini).
int serve_mini(int listen_fd, Store& st);

/// Single-thread kqueue event loop (SERVER=kq).
int serve_kq(int listen_fd, Store& st);

/// Shared-port listener: SO_REUSEADDR + SO_REUSEPORT, dual-stack on :port.
int listen_socket(int port);

} // namespace shrt
