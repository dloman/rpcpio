#include <boost/asio/io_context.hpp>

#include "rpcpio/channel.h"

int main() {
    boost::asio::io_context io_context;
    rpcpio::ChannelOptions options;
    options.use_tls = false;
    options.use_h2c = true;
    rpcpio::Channel channel(
        io_context, "127.0.0.1", 1, options);
    channel.Shutdown();
    return 0;
}
