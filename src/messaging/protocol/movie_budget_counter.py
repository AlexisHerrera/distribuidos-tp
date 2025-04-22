from src.messaging.protobuf import movie_budget_counter_pb2


class MovieBudgetCounterProtocol:
    def to_bytes(self, data: dict[str, int]) -> tuple[bytes, int]:
        try:
            result_pb = movie_budget_counter_pb2.MovieBudgetCounter()
            result_pb.country_budgets.update(data)
            encoded_bytes = result_pb.SerializeToString()
            return encoded_bytes, len(encoded_bytes)
        except Exception:
            return b'', 0

    def from_bytes(self, buf: bytes, bytes_amount: int) -> dict[str, int] | None:
        try:
            result_pb = movie_budget_counter_pb2.MovieBudgetCounter()
            result_pb.ParseFromString(buf[0:bytes_amount])
            return dict(result_pb.country_budgets)

        except Exception:
            return None
