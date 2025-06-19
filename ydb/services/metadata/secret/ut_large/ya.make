UNITTEST_FOR(ydb/services/metadata/secret)

FORK_SUBTESTS()

TAG(ya:fat)
SIZE(LARGE)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    metadata_acl_ut.cpp
)

END()
