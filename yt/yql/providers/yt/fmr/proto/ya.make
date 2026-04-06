PROTO_LIBRARY()

SRCS(
    coordinator.proto
    request_options.proto
    table_data_service.proto
)

PEERDIR(
    ydb/public/api/protos/annotations
)

EXCLUDE_TAGS(GO_PROTO)

END()
