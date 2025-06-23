def rotate(elements: list, index: int) -> list:
    return elements[index:] + elements[:index]


class IncrementerStop:
    def __init__(self, base_value: int, stop: int):
        self.base_value = base_value
        self.stop = stop

    def run(self) -> bool:
        condition = self.base_value < self.stop
        self.base_value += 1
        return condition
