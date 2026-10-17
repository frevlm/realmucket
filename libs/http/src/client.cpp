#include <curl/curl.h>

#include <mutex>
#include <string>

#include "rmk/http/http.hpp"

namespace rmk::http {
namespace {

std::size_t collect(char* data, std::size_t size, std::size_t count, void* out) {
    static_cast<std::string*>(out)->append(data, size * count);
    return size * count;
}

struct SList {
    curl_slist* list = nullptr;
    ~SList() { curl_slist_free_all(list); }
};

struct Easy {
    CURL* handle = curl_easy_init();
    ~Easy() {
        if (handle) curl_easy_cleanup(handle);
    }
};

}  // namespace

Result<std::string> get(std::string_view url, const GetOptions& options) {
    static std::once_flag once;
    std::call_once(once, [] { curl_global_init(CURL_GLOBAL_DEFAULT); });

    Easy easy;
    if (!easy.handle) return fail(Errc::unavailable, "curl_easy_init failed");
    CURL* c = easy.handle;

    const std::string u(url);
    std::string body;
    SList headers;
    std::string auth;
    if (!options.bearer_token.empty()) {
        auth = "Authorization: Bearer " + options.bearer_token;
        headers.list = curl_slist_append(headers.list, auth.c_str());
    }

    curl_easy_setopt(c, CURLOPT_URL, u.c_str());
    curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, 0L);
    curl_easy_setopt(c, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(c, CURLOPT_TIMEOUT_MS, static_cast<long>(options.timeout.count()));
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, collect);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &body);
    if (headers.list) curl_easy_setopt(c, CURLOPT_HTTPHEADER, headers.list);
    if (!options.ca_file.empty()) curl_easy_setopt(c, CURLOPT_CAINFO, options.ca_file.c_str());

    const CURLcode rc = curl_easy_perform(c);
    if (rc != CURLE_OK)
        return fail(Errc::unavailable, "GET " + u + ": " + curl_easy_strerror(rc));

    long status = 0;
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &status);
    if (status >= 200 && status < 300) return body;

    const std::string msg = "GET " + u + ": HTTP " + std::to_string(status);
    if (status >= 500) return fail(Errc::unavailable, msg);
    if (status == 404) return fail(Errc::not_found, msg);
    if (status == 401 || status == 403) return fail(Errc::permission_denied, msg);
    return fail(Errc::invalid_argument, msg);
}

}  // namespace rmk::http
