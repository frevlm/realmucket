# realmucket Client SDK

One header, `rmk/client.hpp`; link `rmk_client`. Every call returns `Result<T>` (`std::expected<T, Error>`).

## Connect

```cpp
#include "rmk/client.hpp"
namespace rmk = ::rmk::client;

rmk::Options o;
o.endpoint = "gateway.example:443";
o.ca_file = "/etc/rmk/ca.pem";     // empty = system roots
auto client = rmk::Client::connect(o);
if (!client) { /* client.error() */ }
```

Access control is done by k8s; inside a cluster the Gateway does not authenticate callers.
`o.insecure = true` selects plaintext (development only).

## Put and get

```cpp
rmk::Location where{"acme", "photos", "cat.jpg"};   // the tenant is always required
client->put(where, "hello");
client->put_file(where, "/data/big.bin");
auto text = client->get(where);                  // std::string
std::ofstream out("cat.jpg");
client->get(where, out);                         // streamed
```

A full read verifies the object's crc32c and fails with `ErrorCode::corruption` on mismatch.
Reads with `generation`, `offset` or `length` set are not verified.

## Conditional write

```cpp
client->put(where, "v1", rmk::Precondition::if_not_exists());
auto info = client->head(where);
client->put(where, "v2", rmk::Precondition::if_generation(info->generation));
// ErrorCode::precondition_failed if someone wrote in between
```

## Multipart and resume

Content above `multipart_threshold` is split into `part_size` Parts, `parallel_parts` in flight
(at most `parallel_parts * part_size` bytes buffered). If it fails, the upload is kept and its id is
in the error message. Record the id from the progress callback and resume:

```cpp
std::uint64_t id = 0;
auto r = client->put_file(where, path, {}, std::nullopt,
                          [&](auto upload_id, auto part, auto bytes) { id = upload_id; });
if (!r) {
    client->put_file(where, path, {}, id);   // skips Parts already stored with the right size
    // or client->abort_upload(id);
}
```

Keep `part_size` the same when resuming.

## Errors

`Error::code` is parsed from the Gateway's `"<code>: message"` status text; anything else is
`ErrorCode::unknown` with the full message. `unavailable` and `aborted` are worth retrying.

## rmkctl

```
export RMK_ENDPOINT=gateway:443 RMK_TENANT=acme
rmkctl bucket create photos --versioning
rmkctl object put photos cat.jpg ./cat.jpg --if-not-exists
rmkctl object get photos cat.jpg -
rmkctl object ls photos --prefix cat
```

Exit status is 0, or 1 with `error: <code>: <message>` on stderr. Run `rmkctl` without arguments for the full list.
