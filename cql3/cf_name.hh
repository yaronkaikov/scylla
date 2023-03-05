/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include "cql3/keyspace_element_name.hh"

namespace cql3 {

class cf_name final : public keyspace_element_name {
    sstring _cf_name = "";
public:
    void set_column_family(const sstring& cf, bool keep_case);

    const sstring& get_column_family() const;

    virtual sstring to_string() const override;
};

inline
std::ostream&
operator<<(std::ostream& os, const cf_name& n) {
    os << n.to_string();
    return os;
}

}
