from collections import OrderedDict

# Indicates start of integers
TOKEN_INTEGER = b'i'

# Indicates start of list
TOKEN_LIST = b'l'

# Indicates start of dict
TOKEN_DICT = b'd'

# Indicate end of lists, dicts and integer values
TOKEN_END = b'e'

# Delimits string length from string data
TOKEN_STRING_SEPARATOR = b':'

class Decoder:
    """
        Decodes a bencoding stream into a python object
    """
    def __init__(self, data: bytes):
        if not isinstance(data, bytes):
            raise TypeError("Argument 'data' must be of type bytes")
        self._data = data
        self._index = 0

    def decode(self):
        """
            Decodes the bencoded data and return the matching python objecte

            :return A python object representing the bencoded data
        """
        c = self._peek()

class Encoder:
    """
        Encodes a python object into bencoding format
    """
    def __init__(self, data):
        self.data = data
        self.encoded_data = None

    def encode(self) -> bytes:
        if self.encoded_data is not None:
            return self.encoded_data
        self.encoded_data = self._encode_data_type(self.data)
        return self.encoded_data

    def _encode_data_type(self, data):
        data_type = type(data)
        if data_type in [dict, OrderedDict]:
            return self._encode_dict(data)
        elif data_type == list:
            return self._encode_list(data)
        elif data_type == int:
            return self._encode_int(data)
        elif data_type == str:
            return self._encode_str(data)
        elif data_type == bytes:
            return self._encode_bytes(data)
        else:
            return None
            # raise TypeError(f"Python object of type {data_type} cannot be encoded as benconding")

    def _encode_dict(self, data):
        encoded_list = []
        for key, value in data.items():
            encoded_list.append(self._encode_data_type(key).decode("utf-8"))
            encoded_list.append(self._encode_data_type(value).decode("utf-8"))
        return bytes(f"d{''.join(encoded_list)}e", encoding="utf-8")

    def _encode_bytes(self, value: str):
        return bytes(f"{len(value):{value}}", encoding="utf-8")

    def _encode_list(self, data):
        encoded_list = [self._encode_data_type(x).decode("utf-8") for x in data]
        return bytes(f"l{''.join(encoded_list)}e", encoding="utf-8")

    def _encode_str(self, data):
        return bytes(f"{len(data)}:{data}", encoding="utf-8")

    def _encode_int(self, data):
        return bytes(f"i{data}e", encoding="utf-8")
