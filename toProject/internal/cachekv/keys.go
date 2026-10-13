package cachekv

const GRPCPrefix = "cache:grpc:"

const GroupIndexPrefix = "cache:idx:"

func GroupIndex(group string) string {
	return GroupIndexPrefix + group
}
