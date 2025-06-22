def rotate(elements: list, index: int) -> list:
    return elements[index:] + elements[:index]
