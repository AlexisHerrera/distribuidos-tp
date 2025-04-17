class ListEncoder:
    def __init__(self, item_to_bytes, encode_all):
        self.__item_to_bytes = item_to_bytes
        self.__encode_all = encode_all

    def to_bytes(self, items) -> tuple[bytes, int]:
        items_pb2 = []

        for item in items:
            items_pb2.append(self.__item_to_bytes(item))

        items_encoded = self.__encode_all(items_pb2)

        return items_encoded, len(items_encoded)
