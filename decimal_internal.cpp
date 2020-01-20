#include "decimal_internal.h"

#include <alloca.h>
#include <endian.h>
#include <cassert>
#include <algorithm>
#include <cstring>
#include <ostream>

#if __BYTE_ORDER != __LITTLE_ENDIAN
#error "only little endian platform is supported"
#endif

#define UNLIKELY(x)  __builtin_expect (!!(x), 0)
#define LIKELY(x)  __builtin_expect (!!(x), 1)

namespace
{
    using slave::decimal::digit_t;

    inline digit_t unpack_n(const char* s, size_t n, digit_t mask)
    {
        digit_t v;
        std::memcpy(&v, s, std::min(sizeof(v), n));
        v ^= mask;
        v <<= (4u - n) * 8u;
        return be32toh(v);
    }

    inline digit_t unpack4(const char* s, digit_t mask)
    {
        return unpack_n(s, 4, mask);
    }

    inline void make_zero(slave::decimal::Decimal& d)
    {
        memset(&d, 0, sizeof(d));
    }

    inline slave::decimal::error on_error(slave::decimal::error err, slave::decimal::Decimal& d)
    {
        assert(0 && "unexpected error");
        make_zero(d);
        return err;
    }
} // namespace

/*
    for storage decimal numbers are converted to the "binary" format.

    This format has the following properties:
      1. length of the binary representation depends on the {precision, scale}
      as provided by the caller and NOT on the intg/frac of the decimal to
      convert.
      2. binary representations of the same {precision, scale} can be compared
      with memcmp - with the same result as decimal_cmp() of the original
      decimals (not taking into account possible precision loss during
      conversion).

    This binary format is as follows:
      1. First the number is converted to have a requested precision and scale.
      2. Every full DIG_PER_DEC1 digits of intg part are stored in 4 bytes
         as is
      3. The first intg % DIG_PER_DEC1 digits are stored in the reduced
         number of bytes (enough bytes to store this number of digits -
         see dig2bytes)
      4. same for frac - full decimal_digit_t's are stored as is,
         the last frac % DIG_PER_DEC1 digits - in the reduced number of bytes.
      5. If the number is negative - every byte is inversed.
      6. The very first bit of the resulting byte array is inverted (because
         memcmp compares unsigned bytes, see property 2 above)

    Example:

      1234567890.1234

    internally is represented as 3 decimal_digit_t's

      1 234567890 123400000

    (assuming we want a binary representation with precision=14, scale=4)
    in hex it's

      00-00-00-01  0D-FB-38-D2  07-5A-EF-40

    now, middle decimal_digit_t is full - it stores 9 decimal digits. It goes
    into binary representation as is:


      ...........  0D-FB-38-D2 ............

    First decimal_digit_t has only one decimal digit. We can store one digit in
    one byte, no need to waste four:

                01 0D-FB-38-D2 ............

    now, last digit. It's 123400000. We can store 1234 in two bytes:

                01 0D-FB-38-D2 04-D2

    So, we've packed 12 bytes number in 7 bytes.
    And now we invert the highest bit to get the final result:

                81 0D FB 38 D2 04 D2

    And for -1234567890.1234 it would be

                7E F2 04 37 2D FB 2D
*/

slave::decimal::error slave::decimal::from_binary(const char* from, Decimal& to, unsigned precision, unsigned scale)
{
    const unsigned max_len = static_cast<unsigned>(Decimal::max_digits);
    if (UNLIKELY(precision < scale))
    {
        return on_error(ERR_DECIMAL_BAD_NUM, to);
    }

    if (UNLIKELY(precision > max_len))
    {
        return on_error(ERR_DECIMAL_OVERFLOW, to);
    }


    const digit_t mask_common = (0 != (from[0] & 0x80)) ? 0 : -1;
    const digit_t mask_first = mask_common ^ 0x80; // mask for first digit
    digit_t mask = mask_first;

    unsigned intg = precision - scale, frac = scale;
    unsigned intg0 = intg / MAX_DIGITS_PER_WORD, frac0 = frac / MAX_DIGITS_PER_WORD,
             intg0x = intg % MAX_DIGITS_PER_WORD, frac0x = frac % MAX_DIGITS_PER_WORD;

    uint8_t* buf = to.storage;
    uint8_t* buf0 = buf;

    if (0 != intg0x)
    {
        uint8_t count = digits2bytes[intg0x];
        digit_t x = unpack_n(from, count, mask);
        mask = mask_common; // override mask
        from += count;
        if (UNLIKELY(x >= powers10[intg0x + 1]))
        {
            return on_error(ERR_DECIMAL_BAD_NUM, to);
        }

        if (0 != x)
            std::memcpy(buf + sizeof(Decimal::storage) - count, &x, count);
        else
            intg -= intg0x;
    }

    for (const char* stop = from + intg0 * sizeof(digit_t); from < stop; from += sizeof(digit_t))
    {
        digit_t x = unpack4(from, mask);
        mask = mask_common; // override mask if it was not overrided before
        if (UNLIKELY(x > DIGIT_MAX))
        {
            return on_error(ERR_DECIMAL_BAD_NUM, to);
        }

        // integer part are walked from high digit to low digit, so we can drop out zero digits in high digits
        // and decrease number of digits accordingly
        if (0 != x || buf0 != buf)
        {
            std::memcpy(buf, &x, sizeof(x));
            buf += sizeof(x);
        }
        else
        {
            // if there was no significant high digit, decrease intg
            intg -= MAX_DIGITS_PER_WORD;
        }
    }

    for (const char* stop = from + frac0 * sizeof(digit_t); from < stop; from += sizeof(digit_t))
    {
        digit_t x = unpack4(from, mask);
        if (UNLIKELY(x > DIGIT_MAX))
        {
            return on_error(ERR_DECIMAL_BAD_NUM, to);
        }

        // there may be no zero digit at end, we cannot drop out zero  digits like in integer part
        // so just copy all digits including zero and do not adjust number of frac digits
        std::memcpy(buf, &x, sizeof(x));
        buf += sizeof(x);
    }

    if (frac0x)
    {
        uint8_t count = digits2bytes[frac0x];
        digit_t x = unpack_n(from, count, mask);
        if (UNLIKELY(x > powers10[frac0x + 1]))
        {
            return on_error(ERR_DECIMAL_BAD_NUM, to);
        }

        if (0 != x)
            std::memcpy(buf, &x, count);
        else
            frac -= frac0x;
    }
    if (intg == 0 && frac == 0)
    {
        make_zero(to);
    }
    else
    {
        to.intg = intg;
        to.frac = frac;
        to.sign = (mask != 0);
    }
    return ERR_DECIMAL_OK;
}

slave::decimal::error slave::decimal::from_string(const char* from, Decimal& to)
{
    const char* s = from;
    while (0 != *s && ' ' == *s) { ++s; }
    if (0 == *s)
    {
        return on_error(ERR_DECIMAL_BAD_NUM, to);
    }

    to.sign = 0;

    switch (*s)
    {
    case '-':
        to.sign = 1;
        [[fallthrough]];
    case '+':
        ++s;
        break;
    default:
        break;
    }

    const char* s1 = s;
    while (0 != *s && '0' <= *s && *s <= '9') { ++s; }

    size_t intg = (s - s1);
    size_t frac = 0;
    if (0 != *s && '.' == *s)
    {
        const char* e = s + 1;
        while (0 != *e && '0' <= *e && *e <= '9') { ++e; }
        frac = (e - s - 1);
    }

    if (UNLIKELY(0 == (intg + frac)))
    {
        return on_error(ERR_DECIMAL_BAD_NUM, to);
    }

    if (UNLIKELY(intg + frac > Decimal::max_digits))
    {
        return on_error(ERR_DECIMAL_OVERFLOW, to);
    }

    to.intg = static_cast<uint8_t>(intg);
    to.frac = static_cast<uint8_t>(frac);

    size_t intg1 = intg / MAX_DIGITS_PER_WORD;
    size_t intg0 = intg % MAX_DIGITS_PER_WORD;
    size_t frac1 = frac / MAX_DIGITS_PER_WORD;
    size_t frac0 = frac % MAX_DIGITS_PER_WORD;

    s1 = s; // points to '.'
    if (0 != intg1)
    {
        uint8_t* buf = to.storage + sizeof(digit_t) * (intg1 - 1);
        for (size_t i = 0; i != intg1; ++i)
        {
            digit_t x = 0;
            for (size_t j = 0; j != MAX_DIGITS_PER_WORD; ++j)
                x += (*--s - '0') * powers10[j];

            std::memcpy(buf, &x, sizeof(x));
            buf -= sizeof(x);
        }
    }
    if (0 != intg0)
    {
        digit_t x = 0;
        for (size_t j = 0; j != intg0; ++j)
            x += (*--s - '0') * powers10[j];

        size_t count = digits2bytes[intg0];
        std::memcpy(to.storage + sizeof(Decimal::storage) - count, &x, count);
    }

    if (0 != frac)
    {
        digit_t x = 0;
        uint8_t* buf = to.storage + sizeof(digit_t) * intg1;
        for (size_t i = 0; i != frac1; ++i)
        {
            for (size_t j = 0; j != MAX_DIGITS_PER_WORD; ++j)
                x = (*++s1 - '0') + x * 10;

            std::memcpy(buf, &x, sizeof(x));
            buf += sizeof(x);
            x = 0;
        }

        for (size_t j = 0; j != frac0; ++j)
        {
            x = (*++s1 - '0') + x * 10;
        }
        std::memcpy(buf, &x, digits2bytes[frac0]);
    }

    // Avoid returning negative zero
    if (0 != to.sign && 0 == to.intg && 0 == to.frac)
        to.sign = 0;

    return ERR_DECIMAL_OK;
}
