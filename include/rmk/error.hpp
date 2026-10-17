#pragma once

#include <expected>
#include <string>
#include <utility>

namespace rmk {

enum class Errc {
    not_found,
    already_exists,
    precondition_failed,
    not_empty,
    invalid_argument,
    expired,
    no_space,
    quota_exceeded,
    permission_denied,
    unauthenticated,
    unavailable,
    aborted,  // lost a race (lease, revision); retrying may succeed
    corruption,
    io_error,
};

inline const char* errc_name(Errc code) {
    switch (code) {
        case Errc::not_found: return "not_found";
        case Errc::already_exists: return "already_exists";
        case Errc::precondition_failed: return "precondition_failed";
        case Errc::not_empty: return "not_empty";
        case Errc::invalid_argument: return "invalid_argument";
        case Errc::expired: return "expired";
        case Errc::no_space: return "no_space";
        case Errc::quota_exceeded: return "quota_exceeded";
        case Errc::permission_denied: return "permission_denied";
        case Errc::unauthenticated: return "unauthenticated";
        case Errc::unavailable: return "unavailable";
        case Errc::aborted: return "aborted";
        case Errc::corruption: return "corruption";
        case Errc::io_error: return "io_error";
    }
    return "unknown";
}

struct Error {
    Errc code;
    std::string message;
};

template <class T>
using Result = std::expected<T, Error>;
using Status = std::expected<void, Error>;

inline std::unexpected<Error> fail(Errc code, std::string message) {
    return std::unexpected(Error{code, std::move(message)});
}

}  // namespace rmk
