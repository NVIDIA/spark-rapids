# A JSON generator built based on the context free grammar from https://www.json.org/json-en.html

# This is just a simple prototype of JSON generator.
# Run
# ```python
# with open("./temp.json", 'w') as f:
#     for t in json_gen():
#         f.write(t)
# ```
# to generate a random JSON file.

import random

def json_gen():
    """
    JSON -> ELEMENT
    """
    for t in element_gen():
        yield t

def element_gen():
    """
    ELEMENT -> WHITESPACE VALUE WHITESPACE
    """
    for t in whitespace_gen():
        yield t
    for t in value_gen():
        yield t
    for t in whitespace_gen():
        yield t

def value_gen():
    """
    VALUE -> OBJECT 
           | ARRAY (todo) 
           | STRING (todo) 
           | NUMBER (todo) 
           | BOOL
    """
    for t in random.choices([object_gen(), bool_gen()], weights=[70, 30], k=1)[0]:
        yield t

def object_gen():
    """
    OBJECT -> '{' WHITESPACE '}' 
            | '{' MEMBERS '}'
    """
    yield "{"
    for t in random.choices([whitespace_gen(), members_gen()], weights=[30, 70], k=1)[0]:
        yield t
    yield "}"

def members_gen():
    """
    MEMBERS -> MEMBER 
             | MEMBER ',' MEMBERS
    """
    if random.randint(0,100) < 50:
        for t in member_gen():
            yield t
    else:
        for t in member_gen():
            yield t
        yield ","
        for t in members_gen():
            yield t

def member_gen():
    """
    MEMBER -> WHITESPACE STRING WHITESPACE ':' ELEMENT
    """
    for t in whitespace_gen():
        yield t
    for t in string_gen():
        yield t
    for t in whitespace_gen():
        yield t
    yield ":"
    for t in element_gen():
        yield t

def string_gen():
    """
    STRING -> '"' CHARACTERS '"'
    """
    yield '"'
    for t in characters_gen():
        yield t
    yield '"'

def characters_gen():
    """
    CHARACTERS -> '' 
                | CHAR CHARACTERS
    """
    if random.randint(0,100) < 30:
        yield ''
    else:
        for t in char_gen():
            yield t
        for t in characters_gen():
            yield t

def char_gen():
    """
    CHAR -> 0x0020 .. 0x10ffff (exclude 0x0022 and 0x005c) 
          | '\\' ESCAPE
    """
    if random.randint(0, 99) < 80:
        unicode = random.randint(0x0020, 0x10ffff)
        while unicode == 0x22 or unicode == 0x5c:
            unicode = random.randint(0x0020, 0x10ffff)
        yield chr(unicode)
    
    else:
        yield '\\'
        for t in escape_gen():
            yield t
    

def escape_gen():
    """
    ESCAPE -> '"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't' 
            | 'u' HEX HEX HEX HEX
    """
    if random.randint(0, 8) < 8:
        yield random.choice(['"', '\\', '/', 'b', 'f', 'n', 'r', 't'])
    else:
        yield 'u'
        for _ in range(4):
            for t in hex_gen():
                yield t

def hex_gen():
    """
    HEX -> DIGIT 
         | 'a' .. 'f' 
         | 'A' .. 'F'
    """
    path = random.randint(0, 2)
    if path == 0:
        for t in digit_gen():
            yield t
    elif path == 1:
        yield chr(random.randint(0x41, 0x46))
    else:
        yield chr(random.randint(0x61, 0x66))

def digit_gen():
    """
    DIGIT -> '0' 
           | ONENINE
    """
    if random.randint(0, 9) == 0:
        yield '0'
    else:
        for t in onenine_gen():
            yield t

def onenine_gen():
    """
    ONENINE -> '1' .. '9'
    """
    yield chr(random.randint(0x31, 0x39))

def whitespace_gen():
    """
    WHITESPACE -> '' 
                | 0x0020 WHITESPACE 
                | 0x000a WHITESPACE (todo)
                | 0x000d WHITESPACE 
                | 0x0009 WHITESPACE (todo)
    """
    if random.randint(0, 4) > 3:
        yield chr(random.choice([0x20, 0xD]))
        for t in whitespace_gen():
            yield t
    else:
        yield ''

def bool_gen():
    """
    BOOL -> "true" | "null" | "false"
    """
    yield random.choice(["true", "null", "false"])