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
        if c is None:
            raise EOFError("Unexpected end-of-file")
        elif c == TOKEN_INTEGER:
            self._consume()
            return self._decode_int()
        elif c == TOKEN_LIST:
            self._consume()
            return self._decode_list()
        elif c == TOKEN_DICT:
            self._consume()
            return self._decode_dict()
        elif c == TOKEN_END:
            return None
        elif c in b'0123456789':
            return self._decode_string()
        else:
            raise RuntimeError(f"Invalid token read at {self._index}")

    def _peek(self):
        """
            Return the next character from the bencoded data or None
        """
        if self._index+1 >= len(self._data):
            return None
        return self._data[self._index:self._index + 1]

    def _consume(self):
        """
            Read the next character from the data
        """
        self._index += 1

    def _read(self, length: int) -> bytes:
        """
            Read the `length` number of bytes from data and return the result
        """
        if self._index + length > len(self._data):
            raise IndexError(f"Cannot read {length} bytes from position {self._index}")
        res = self._data[self._index:self._index+length]
        self._index += length
        return res

    def _read_until(self, token: bytes) -> bytes:
        """
            Read from the bencoded data until the given token is found and
            return the characters read
        """
        try:
            occurrence = self._data.index(token, self._index)
            result = self._data[self._index:occurrence]
            self._index = occurrence + 1
            return result
        except ValueError:
            raise RuntimeError(f"Unable to find token {token}")

    def _decode_int(self):
        return int(self._read_until(TOKEN_END))

    def _decode_list(self):
        res = []
        # recursively decode the contents of the list
        while self._data[self._index:self._index+1] != TOKEN_END:
            res.append(self.decode())
        self._consume()  # cosume end token 
        return res

    def _decode_dict(self):
        res = OrderedDict()
        while self._data[self._index:self._index+1] != TOKEN_END:
            key = self.decode()
            value = self.decode()
            res[key] = value
        self._consume()  # cosume end token 
        return res

    def _decode_string(self):
        bytes_to_read = int(self._read_until(TOKEN_STRING_SEPARATOR))
        data = self._read(bytes_to_read)
        return data

class Encoder:
    """
        Encodes a python object into bencoding format
    """
    def __init__(self, data):
        self.data = data

    def encode(self) -> bytes:
        return self._encode_data_type(self.data)

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
        result = bytearray("d", "utf-8")
        for key, value in data.items():
            k = self._encode_data_type(key)
            v = self._encode_data_type(value)
            if k and v:
                result += k
                result += v
            else:
                raise RuntimeError("Bad dict")
        result += b"e"
        return result

    def _encode_bytes(self, value: bytes):
        result = bytearray()
        result += str.encode(str(len(value)))
        result += b':'
        result += value
        return result

    def _encode_list(self, data):
        encoded_list = bytearray("l", "utf-8")
        encoded_list += b"".join([self._encode_data_type(x) for x in data])
        encoded_list += b"e"
        return encoded_list

    def _encode_str(self, data):
        return bytes(f"{len(data)}:{data}", encoding="utf-8")

    def _encode_int(self, data):
        return bytes(f"i{data}e", encoding="utf-8")
