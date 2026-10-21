/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "folly/CPortability.h"

#include "velox/functions/Macros.h"

namespace facebook::velox::functions::flinksql {

// Flink hash expressions: MD5/SHA1/SHA224/SHA256/SHA384/SHA512 take a string
// (hashed as its UTF-8 bytes) and return a lowercase hex VARCHAR — Flink
// anchors on commons-codec DigestUtils.*Hex. The baselines' md5/sha1/sha256
// take and return varbinary raw digests, so the flinksql signatures dispatch
// on the input type and coexist with them. Digest primitives:
// crypto::MD5Context (velox/external/md5) and folly::ssl::OpenSSLHash.
//
// Implementation pattern: one struct per function with
// VELOX_DEFINE_FUNCTION_TYPES, registered with explicit signatures in
// registration/HashFunctionsRegistration.cpp (see velox/functions/prestosql
// for the convention).

// TODO: implement the Flink hash expressions here.

} // namespace facebook::velox::functions::flinksql
