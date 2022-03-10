# A JSON generator built based on the context free grammar from https://www.json.org/json-en.html


import random
from data_gen import *

def gen_schema():
    fields = random.randint(1, 5)
    name_gen = StringGen(nullable= False)
    name_gen.start(random)
    return StructType([StructField(name_gen.gen(), StringType()) for _ in range(fields)])

# This is just a simple prototype of JSON generator.
# Run
# ```python
# schema = gen_schema()
# with open("./temp.json", 'w') as f:
#     for t in gen_json(schema):
#         f.write(t)
# ```
# to generate a random JSON file.
def gen_json(schema: DataType):
    """
    JSON -> ELEMENT
    """
    for t in gen_element(schema):
        yield t

def gen_element(schema: DataType):
    """
    ELEMENT -> WHITESPACE VALUE WHITESPACE
    """
    for t in gen_whitespace():
        yield t
    for t in gen_value(schema):
        yield t
    for t in gen_whitespace():
        yield t

def gen_value(schema: DataType):
    """
    VALUE -> OBJECT 
           | ARRAY (todo) 
           | STRING (todo) 
           | NUMBER (todo) 
           | BOOL (todo)
    """
    if isinstance(schema, StructType):
        for t in gen_object(schema):
            yield t 
    elif isinstance(schema, StringType):
        for t in gen_string():
            yield t
    else:
        raise Exception("not supported schema")

def gen_object(schema: StructType):
    """
    OBJECT -> '{' WHITESPACE '}' 
            | '{' MEMBERS '}'
    """
    yield "{"
    if len(schema) == 0:
        for t in gen_whitespace():
            yield t
    else:
        for t in gen_members(schema.fields):
                yield t
    yield "}"

def gen_members(schema: list[StructField]):
    """
    MEMBERS -> MEMBER 
             | MEMBER ',' MEMBERS
    """
    if len(schema) == 1:
        for t in gen_member(schema[0]):
            yield t
    else:
        for t in gen_member(schema[0]):
            yield t
        yield ","
        for t in gen_members(schema[1:]):
            yield t

def gen_member(schema: StructField):
    """
    MEMBER -> WHITESPACE STRING WHITESPACE ':' ELEMENT
    """
    for t in gen_whitespace():
        yield t

    yield schema.name

    for t in gen_whitespace():
        yield t

    yield ":"

    for t in gen_element(schema.dataType):
        yield t

def gen_string():
    """
    STRING -> '"' CHARACTERS '"'
    """
    yield '"'
    for t in gen_characters():
        yield t
    yield '"'

def gen_characters():
    """
    CHARACTERS -> '' 
                | CHAR CHARACTERS
    """
    if random.randint(0,100) < 30:
        yield ''
    else:
        for t in gen_char():
            yield t
        for t in gen_characters():
            yield t

def gen_char():
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
        for t in gen_escape():
            yield t
    

def gen_escape():
    """
    ESCAPE -> '"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't' 
            | 'u' HEX HEX HEX HEX
    """
    if random.randint(0, 8) < 8:
        yield random.choice(['"', '\\', '/', 'b', 'f', 'n', 'r', 't'])
    else:
        yield 'u'
        for _ in range(4):
            for t in gen_hex():
                yield t

def gen_hex():
    """
    HEX -> DIGIT 
         | 'a' .. 'f' 
         | 'A' .. 'F'
    """
    path = random.randint(0, 2)
    if path == 0:
        for t in gen_digit():
            yield t
    elif path == 1:
        yield chr(random.randint(0x41, 0x46))
    else:
        yield chr(random.randint(0x61, 0x66))

def gen_digit():
    """
    DIGIT -> '0' 
           | ONENINE
    """
    if random.randint(0, 9) == 0:
        yield '0'
    else:
        for t in gen_onenine():
            yield t

def gen_onenine():
    """
    ONENINE -> '1' .. '9'
    """
    yield chr(random.randint(0x31, 0x39))

def gen_whitespace():
    """
    WHITESPACE -> '' 
                | 0x0020 WHITESPACE 
                | 0x000a WHITESPACE (todo)
                | 0x000d WHITESPACE 
                | 0x0009 WHITESPACE (todo)
    """
    if random.randint(0, 4) > 3:
        yield chr(random.choice([0x20, 0xD]))
        for t in gen_whitespace():
            yield t
    else:
        yield ''

def gen_bool():
    """
    BOOL -> "true" | "null" | "false"
    """
    yield random.choice(["true", "null", "false"])

def test_json_gen():
    schema = gen_schema()
    with open("./temp.json", 'w') as f:
        for t in gen_json(schema):
            f.write(t)
    assert 1 == 2