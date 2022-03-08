import random

def json_gen():
    for token in element_gen():
        yield token

def element_gen():
    for token in whitespace_gen():
        yield token
    for token in value_gen():
        yield token
    for token in whitespace_gen():
        yield token

def value_gen():
    for token in random.choice([object_gen(), bool_gen()]):
        yield token

def object_gen():
    yield "{"
    for token in random.choice([whitespace_gen(), members_gen()]):
        yield token
    yield "}"

def members_gen():
    if random.randint(0,100) < 50:
        for token in member_gen():
            yield token
    else:
        for token in member_gen():
            yield token
        yield ","
        for token in members_gen():
            yield token

def member_gen():
    for token in whitespace_gen():
        yield token
    for token in string_gen():
        yield token
    for token in whitespace_gen():
        yield token
    yield ":"
    for token in element_gen():
        yield token

def string_gen():
    yield "\""
    for token in characters_gen():
        yield token
    yield "\""

def characters_gen():
    if random.randint(0,100) < 50:
        yield ""
    else:
        for token in char_gen():
            yield token
        for token in characters_gen():
            yield token

def char_gen():
    yield chr(random.randint(0x0020, 0x00ff))

def whitespace_gen():
    yield " "

def bool_gen():
    yield random.choice(["true", "null", "false"])