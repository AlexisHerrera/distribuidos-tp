import ast

def dictionary_to_list(dictionary_str):
    try:
        dictionary_list = ast.literal_eval(dictionary_str)
        return [data['name'] for data in dictionary_list]
    except (ValueError, SyntaxError):
        return []