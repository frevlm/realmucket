#include <array>
#include <string>
#include <string_view>

#include "rmk/rpc/rpc.hpp"

namespace rmk::rpc {
namespace {

constexpr std::array kAllErrc{
    Errc::not_found,         Errc::already_exists, Errc::precondition_failed, Errc::not_empty,
    Errc::invalid_argument,  Errc::expired,        Errc::no_space,            Errc::quota_exceeded,
    Errc::permission_denied, Errc::unauthenticated, Errc::unavailable,        Errc::aborted,
    Errc::corruption,        Errc::io_error,
};

grpc::StatusCode code_of(Errc code) {
    switch (code) {
        case Errc::not_found: return grpc::StatusCode::NOT_FOUND;
        case Errc::already_exists: return grpc::StatusCode::ALREADY_EXISTS;
        case Errc::precondition_failed:
        case Errc::not_empty:
        case Errc::expired: return grpc::StatusCode::FAILED_PRECONDITION;
        case Errc::invalid_argument: return grpc::StatusCode::INVALID_ARGUMENT;
        case Errc::no_space:
        case Errc::quota_exceeded: return grpc::StatusCode::RESOURCE_EXHAUSTED;
        case Errc::permission_denied: return grpc::StatusCode::PERMISSION_DENIED;
        case Errc::unauthenticated: return grpc::StatusCode::UNAUTHENTICATED;
        case Errc::unavailable: return grpc::StatusCode::UNAVAILABLE;
        case Errc::aborted: return grpc::StatusCode::ABORTED;
        case Errc::corruption: return grpc::StatusCode::DATA_LOSS;
        case Errc::io_error: return grpc::StatusCode::INTERNAL;
    }
    return grpc::StatusCode::INTERNAL;
}

// For statuses that did not come from to_status (transport failures, other servers).
Errc errc_of(grpc::StatusCode code) {
    switch (code) {
        case grpc::StatusCode::NOT_FOUND: return Errc::not_found;
        case grpc::StatusCode::ALREADY_EXISTS: return Errc::already_exists;
        case grpc::StatusCode::FAILED_PRECONDITION: return Errc::precondition_failed;
        case grpc::StatusCode::INVALID_ARGUMENT:
        case grpc::StatusCode::OUT_OF_RANGE: return Errc::invalid_argument;
        case grpc::StatusCode::RESOURCE_EXHAUSTED: return Errc::no_space;
        case grpc::StatusCode::PERMISSION_DENIED: return Errc::permission_denied;
        case grpc::StatusCode::UNAUTHENTICATED: return Errc::unauthenticated;
        case grpc::StatusCode::UNAVAILABLE:
        case grpc::StatusCode::DEADLINE_EXCEEDED:
        case grpc::StatusCode::CANCELLED: return Errc::unavailable;
        case grpc::StatusCode::ABORTED: return Errc::aborted;
        case grpc::StatusCode::DATA_LOSS: return Errc::corruption;
        default: return Errc::io_error;
    }
}

}  // namespace

grpc::Status to_status(const Error& error) {
    return grpc::Status(code_of(error.code), std::string(errc_name(error.code)) + ": " + error.message);
}

Error from_status(const grpc::Status& status) {
    const std::string& text = status.error_message();
    for (Errc code : kAllErrc) {
        std::string_view name = errc_name(code);
        if (text.size() >= name.size() + 2 && text.starts_with(name) &&
            std::string_view(text).substr(name.size(), 2) == ": " && code_of(code) == status.error_code())
            return Error{code, text.substr(name.size() + 2)};
    }
    if (status.ok())
        return Error{Errc::io_error, "unexpected OK status"};
    return Error{errc_of(status.error_code()), text};
}

}  // namespace rmk::rpc
