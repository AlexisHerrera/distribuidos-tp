class ListDecoder:
    def __init__(self, bytes_to_item, decode_all):
        self.__bytes_to_item = bytes_to_item
        self.__decode_all = decode_all

    def from_bytes(self, buf: bytes, bytes_amount: int) -> list:
        items = self.__decode_all(buf, bytes_amount)

        items_list = []

        for item in items:
            items_list.append(self.__bytes_to_item(item))

        return items_list
