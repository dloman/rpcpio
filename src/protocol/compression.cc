#include "compression.h"

#include <cstring>

// Include zlib if available at compile time.
#ifdef RPCPIO_ENABLE_GZIP
#  include <zlib.h>
#endif

namespace rpcpio::protocol {

bool GzipAvailable() noexcept {
#ifdef RPCPIO_ENABLE_GZIP
    return true;
#else
    return false;
#endif
}

std::optional<Encoding> ParseEncoding(std::string_view s) noexcept {
    if (s == "identity") return Encoding::kIdentity;
#ifdef RPCPIO_ENABLE_GZIP
    if (s == "gzip")     return Encoding::kGzip;
#endif
    return std::nullopt;
}

std::string_view EncodingName(Encoding enc) noexcept {
    switch (enc) {
        case Encoding::kIdentity: return "identity";
        case Encoding::kGzip:     return "gzip";
    }
    return "identity";
}

std::string AcceptEncodingValue() {
    std::string v = "identity";
#ifdef RPCPIO_ENABLE_GZIP
    v += ",gzip";
#endif
    return v;
}

bool Compress(Encoding enc, std::string_view src, std::string& out) {
    if (enc == Encoding::kIdentity) {
        out.assign(src.data(), src.size());
        return true;
    }
#ifdef RPCPIO_ENABLE_GZIP
    if (enc == Encoding::kGzip) {
        z_stream zs{};
        if (deflateInit2(&zs, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                         15 + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
            return false;
        }
        out.resize(src.size() + 64);  // initial estimate
        zs.next_in  = reinterpret_cast<Bytef*>(const_cast<char*>(src.data()));
        zs.avail_in = static_cast<uInt>(src.size());
        zs.next_out  = reinterpret_cast<Bytef*>(out.data());
        zs.avail_out = static_cast<uInt>(out.size());
        int ret = deflate(&zs, Z_FINISH);
        if (ret == Z_BUF_ERROR) {
            // Output buffer too small: grow and retry.
            out.resize(out.size() * 2);
            zs.next_out  = reinterpret_cast<Bytef*>(out.data()) + zs.total_out;
            zs.avail_out = static_cast<uInt>(out.size() - zs.total_out);
            ret = deflate(&zs, Z_FINISH);
        }
        deflateEnd(&zs);
        if (ret != Z_STREAM_END) return false;
        out.resize(zs.total_out);
        return true;
    }
#endif
    return false;  // unsupported encoding
}

bool Decompress(Encoding enc,
                std::string_view src,
                std::string& out,
                std::size_t  max_output_size)
{
    if (enc == Encoding::kIdentity) {
        if (src.size() > max_output_size) return false;
        out.assign(src.data(), src.size());
        return true;
    }
#ifdef RPCPIO_ENABLE_GZIP
    if (enc == Encoding::kGzip) {
        z_stream zs{};
        if (inflateInit2(&zs, 15 + 16) != Z_OK) return false;

        out.resize(std::min(src.size() * 4, max_output_size));
        zs.next_in  = reinterpret_cast<Bytef*>(const_cast<char*>(src.data()));
        zs.avail_in = static_cast<uInt>(src.size());

        int ret = Z_OK;
        while (ret == Z_OK) {
            zs.next_out  = reinterpret_cast<Bytef*>(out.data()) + zs.total_out;
            zs.avail_out = static_cast<uInt>(out.size() - zs.total_out);
            if (zs.avail_out == 0) {
                std::size_t new_size = out.size() * 2;
                if (new_size > max_output_size) { inflateEnd(&zs); return false; }
                out.resize(new_size);
                continue;
            }
            ret = inflate(&zs, Z_NO_FLUSH);
        }
        inflateEnd(&zs);
        if (ret != Z_STREAM_END) return false;
        out.resize(zs.total_out);
        return true;
    }
#endif
    return false;
}

} // namespace rpcpio::protocol
