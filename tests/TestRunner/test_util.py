import random
import string

alphanum_letters = string.ascii_letters + string.digits

def random_alphanum_string(length):
    return "".join(random.choice(alphanum_letters) for _ in range(length))
