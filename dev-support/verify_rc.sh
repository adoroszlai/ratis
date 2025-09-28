#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script performs basic verification steps on release candidate tarballs.

set -e -u -o pipefail

base_url="https://dist.apache.org/repos/dist/dev"
project="ratis"

version="$1"
rc="$2"
tag="$3"
sha="$4"

files="$(echo apache-ratis-${version}-{bin,src}.tar.gz)"

download() {
  local tarball="$1"
  url="${base_url}/${project}/${version}/${rc}/${tarball}"

  curl --fail --location --remote-name-all --show-error --silent \
      "${url}"{,.asc,.mds,.sha512}
}

verify_signatures() {
  local tarball="$1"

  gpg --verify "${tarball}.asc" "${tarball}"

  gpg --print-md SHA512 "${tarball}" > "${tarball}.sha512.expected"
  gpg --print-mds "${tarball}" > "${tarball}.mds.expected"

  for checksum in mds sha512; do
    diff --brief --report-identical-files "${tarball}.${checksum}.expected" "${tarball}.${checksum}"
  done
}

extract() {
  local tarball="$1"

  tar xzf "${tarball}"
}

for tarball in ${files}; do
  echo "$tarball"
  download "$tarball"
  verify_signatures "$tarball"
  extract "$tarball"
  echo
done
