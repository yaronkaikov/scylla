// Copyright (C) 2023-present ScyllaDB
// SPDX-License-Identifier: ScyllaDB-Proprietary

#pragma once

#include "utils/UUID.hh"

using query_id = utils::tagged_uuid<struct query_id_tag>;
