# FDB Tuple layer typecodes

This document is intended to be the system of record for the allocation of typecodes in the Tuple layer. The source code isn’t good enough because a typecode might be added to one language (or by a customer) before another.

Status: Standard means that all of our language bindings implement this typecode.  
Status: Reserved means that this typecode is not yet used in our standard language bindings, but may be in use by third party bindings or specific applications.  
Status: Deprecated means that a previous layer used this type, but issues with that type code have led us to mark this type code as not to be used.  


### **Null Value**

Typecode: `0x00`
Length: 0 bytes  
Status: Standard

### **Byte String**

Typecode: `0x01`
Length: Variable (terminated by` [\x00]![\xff]`)  
Encoding: `b'\x01' + value.replace(b'\x00', b'\x00\xFF') + b'\x00'`  
Test case: `pack("foo\x00bar") == b'\x01foo\x00\xffbar\x00'`  
Status: Standard

In other words, byte strings are null terminated with null values occurring in the string escaped in an order-preserving way.

### **Unicode String**

Typecode: `0x02`
Length: Variable (terminated by` [\x00]![\xff]`)  
Encoding: `b'\x02' + value.encode('utf-8').replace(b'\x00', b'\x00\xFF') + b'\x00'`  
Test case: `pack( u"F\u00d4O\u0000bar" ) == b'\x02F\xc3\x94O\x00\xffbar\x00'`  
Status: Standard

This is the same way that byte strings are encoded, but first, the unicode string is encoded in UTF-8.

### **(DEPRECATED) Nested Tuple**

Typecodes: `0x03` - `0x04`
Length: Variable (terminated by `0x04` type code)  
Status: Deprecated  

This encoding was used by a few layers. However, it had ordering problems when one tuple was a prefix of another and the type of the first element in the longer tuple was either null or a byte string. For an example, consider the empty tuple and the tuple containing only null. In the old scheme, the empty tuple would be encoded as `\x03\x04` while the tuple containing only null would be encoded as `\x03\x00\x04`, so the second tuple would sort first based on their bytes, which is incorrect semantically.

### **Nested Tuple**

Typecodes: `0x05`
Length: Variable (terminated by `[\x00]![\xff]` at the end of nested element)  
Encoding: `b'\x05' + ''.join(map(lambda x: b'\x00\xff' if x is None else pack(x), value)) + b'\x00'`  
Test case: `pack( ((b"foo\x00bar", None, ()),) ) == b'\x05\x01foo\x00\xffbar\x00\x00\xff\x05\x00\x00'`  
Status: Standard

The list ends with a 0x00 byte. Nulls within the tuple are encoded as `\x00\xff`. There is no other null escaping. In particular, 0x00 bytes that are within the nested types can be left as-is as they are passed over when decoding the interior types. To show how this fixes the bug in the previous version of nested tuples, the empty tuple is now encoded as `\x05\x00` while the tuple containing only null is encoded as `\x05\x00\xff\x00`, so the first tuple will sort first.

### **Negative arbitrary-precision Integer**

Typecodes: `0x0a`, `0x0b`
Encoding: Not defined yet  
Status: Reserved; `0x0b` used in Python and Java

These typecodes are reserved for encoding integers larger than 8 bytes. Presumably the type code would be followed by some encoding of the length, followed by the big endian one’s complement number. Reserving two typecodes for each of positive and negative numbers is probably overkill, but until there’s a design in place we might as well not use them. In the Python and Java implementations, `0x0b` stores negative numbers which are expressed with between 9 and 255 bytes. The first byte following the type code (`0x0b`) is a single byte expressing the number of bytes in the integer (with its bits flipped to preserve order), followed by that number of bytes representing the number in big endian order in one's complement.

### **Integer**

Typecodes: `0x0c` - `0x1c`  
&nbsp;`0x0c` is an 8 byte negative number  
...  
&nbsp;`0x10` is an 4 byte negative number  
...  
&nbsp;`0x12` is an 2 byte negative number  
&nbsp;`0x13` is a 1 byte negative number  
&nbsp;`0x14` is a zero  
&nbsp;`0x15` is a 1 byte positive number  
&nbsp;`0x16` is a 2 byte positive number  
...  
&nbsp;`0x18` is a 4 byte positive number  
...  
&nbsp;`0x1c` is an 8 byte positive number  
Length: Depends on typecode (0-8 bytes)  
Encoding: positive numbers are big endian  
 negative numbers are big endian one’s complement (so -1 is `0x13` `0xfe`)  
Test case: `pack( -5551212 ) == b'\x11\xabK\x93'`  
Status: Standard

There is some variation in the ability of language bindings to encode and decode values at the outside of the possible range, because of different native representations of integers. 

### **Positive arbitrary-precision Integer**

Typecodes: `0x1d`, `0x1e`
Encoding: Not defined yet  
Status: Reserved; `0x1d` used in Python and Java

These typecodes are reserved for encoding integers larger than 8 bytes. Presumably the type code would be followed by some encoding of the length, followed by the big endian one’s complement number. Reserving two typecodes for each of positive and negative numbers is probably overkill, but until there’s a design in place we might as well not use them. In the Python and Java implementations, `0x1d` stores positive numbers which are expressed with between 9 and 255 bytes. The first byte following the type code (`0x1d`) is a single byte expressing the number of bytes in the integer, followed by that number of bytes representing the number in big endian order.

### **IEEE Binary Floating Point**

Typecodes:   
&nbsp;`0x20` - float (32 bits)  
&nbsp;`0x21` - double (64 bits)  
&nbsp;`0x22` - long double (80 bits)  
Length: 4 - 10 bytes  
Test case: `pack( -42f ) == b'\x20\x3d\xd7\xff\xff'`
Encoding: Big-endian IEEE binary representation, followed by the following transformation:  
```python
 if ord(rep[0])&0x80: # Check sign bit
    # Flip all bits, this is easier in most other languages!
    return "".join( chr(0xff^ord(r)) for r in rep )
 else:
    # Flip just the sign bit
    return chr(0x80^ord(rep[0])) + rep[1:]
```
Status: Standard (float and double) ; Reserved (long double)

The binary representation should not be assumed to be canonicalized (as to multiple representations of NaN, for example) by a reader. This order sorts all numbers in the following way:

* All negative NaN values with order determined by mantissa bits (which are semantically meaningless)
* Negative infinity
* All real numbers in the standard order (except that -0.0 < 0.0)
* Positive infinity
* All positive NaN values with order determined by mantissa bits

This should be equivalent to the standard IEEE total ordering.

### **Arbitrary-precision Decimal**

Typecodes: `0x23`, `0x24`
Length: Arbitrary  
Encoding: Scale followed by arbitrary precision integer  
Status: Reserved

This encoding format has been used by layers. Note that this encoding makes almost no guarantees about ordering properties of tuple-encoded values and should thus generally be avoided.

### **(DEPRECATED) True Value**

Typecode: `0x25`
Length: 0 bytes  
Status: Deprecated

### **False Value**

Typecode: `0x26`
Length: 0 bytes  
Status: Standard

### **True Value**

Typecode: `0x27`
Length: 0 bytes  
Status: Standard

Note that false will sort before true with the given encoding.

### **RFC 4122 UUID**

Typecode: `0x30`
Length: 16 bytes  
Encoding: Network byte order as defined in the rfc: [_http://www.ietf.org/rfc/rfc4122.txt_](http://www.ietf.org/rfc/rfc4122.txt)  
Status: Standard

This is equivalent to the unsigned byte ordering of the UUID bytes in big-endian order.

### **64 bit identifier**

Typecode: `0x31`
Length: 8 bytes  
Encoding: Big endian unsigned 8-byte integer (typically random or perhaps semi-sequential)  
Status: Reserved

There’s definitely some question of whether this deserves to be separated from a plain old 64 bit integer, but a separate type was desired in one of the third-party bindings. This type has not been ported over to the first-party bindings.

### **80 Bit versionstamp**

Typecode: `0x32`
Length: 10 bytes  
Encoding: Big endian 10-byte integer. First/high 8 bytes are a database version, next two are batch version.  
Status: Reserved

### **96 Bit Versionstamp**

Typecode: `0x33`
Length: 12 bytes  
Encoding: Big endian 12-byte integer. First/high 8 bytes are a database version, next two are batch version, next two are ordering within transaction.  
Status: Python, Java

The two versionstamp typecodes are reserved for ongoing work adding compatibility between the tuple layer and versionstamp operations. Note that the first 80 bits of the 96 bit versionstamp are the same as the contents of the 80 bit versionstamp, and they correspond to what the `SET_VERSIONSTAMP_KEY` mutation will write into a database key , i.e., the first 8 bytes are a big-endian, unsigned version corresponding to the commit version of a transaction, and the next two bytes are a big-endian, unsigned batch number ordering transactions are committed at the same version. The final two bytes of the 96 bit versionstamp are written by the client and should order writes within a single transaction, thereby providing a global order for all versions.

### **User type codes**

Typecode: `0x40` - `0x4f`
Length: Variable (user defined)  
Encoding: User defined  
Status: Reserved

These type codes may be used by third party extenders without coordinating with us. If used in shipping software, the software should use the directory layer and specify a specific layer name when opening its directories to eliminate the possibility of conflicts.

The only way in which future official, otherwise backward-compatible versions of the tuple layer would be expected to use these type codes is to implement some kind of actual extensibility point for this purpose - they will not be used for standard types.

### **Escape Character**

Typecode: `0xff`
Length: N/A  
Encoding: N/A  
Status: Reserved

This type code is not used for anything. However, several of the other tuple types depend on this type code not being used as a type code for other types in order to correctly escape bytes in an order-preserving way. Therefore, it would be a Very Bad Idea™ for future development to start using this code for anything else.
