# Expression Language Reference

Taxinomia includes a Python-like expression language for computed columns. This document describes the syntax, operators, and built-in functions available.

## Value Types

The expression language supports the following value types:

| Type | Description | Example |
|------|-------------|---------|
| `int` | 64-bit signed integer | `42`, `-100`, `1000000` |
| `float` | 64-bit floating-point number | `3.14`, `-1.5`, `1e10` |
| `string` | Text values | `"hello"`, `'world'` |
| `bool` | Boolean values | `True`, `False` |
| `datetime` | Date and time (Unix nanoseconds) | `date_add(...)` result |
| `duration` | Time duration (nanoseconds) | `duration("2h30m")` |
| `nil` | Null/missing value | `None` |

### Numeric Type Rules

Integer and float literals are distinguished by the presence of a decimal point or exponent:
- `42`, `-100` → integers (int64)
- `3.14`, `1e10`, `42.0` → floats (float64)

Arithmetic operations preserve integer types when both operands are integers:
- `5 + 3` → `8` (int)
- `5 + 3.0` → `8.0` (float)
- `5.0 + 3` → `8.0` (float)

Special cases:
- Division `/` always returns float: `10 / 4` → `2.5`
- Floor division `//` always returns int: `10 // 4` → `2`
- Power `**` returns int if both operands are integers: `2 ** 3` → `8` (int)

## Operators

### Arithmetic Operators

| Operator | Description | Result Type | Example |
|----------|-------------|-------------|---------|
| `+` | Addition (numbers) or concatenation (strings) | Preserves int | `5 + 3` → `8`, `"a" + "b"` → `"ab"` |
| `-` | Subtraction | Preserves int | `10 - 4` → `6` |
| `*` | Multiplication | Preserves int | `6 * 7` → `42` |
| `/` | Division | Always float | `15 / 3` → `5.0`, `17 / 5` → `3.4` |
| `//` | Floor division | Always int | `17 // 5` → `3` |
| `%` | Modulo | Preserves int | `17 % 5` → `2` |
| `**` | Power | Preserves int | `2 ** 3` → `8` |

### Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `==` | Equal | `x == 5` |
| `!=` | Not equal | `x != 0` |
| `<` | Less than | `x < 10` |
| `>` | Greater than | `x > 0` |
| `<=` | Less than or equal | `x <= 100` |
| `>=` | Greater than or equal | `x >= 1` |

Comparison operators work on numbers, strings, datetimes, and durations. Values of different types are never equal.

### Logical Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `and` | Logical AND (short-circuit) | `x > 0 and x < 10` |
| `or` | Logical OR (short-circuit) | `x == 0 or x == 1` |
| `not` | Logical NOT | `not x` |

### Unary Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `-` | Negation | `-x` |
| `not` | Logical NOT | `not x` |

## Datetime and Duration Arithmetic

The expression language supports arithmetic with datetime and duration values:

```
datetime - datetime = duration
datetime + duration = datetime
datetime - duration = datetime
duration + duration = duration
duration - duration = duration
```

## Built-in Functions

### Type Conversion

| Function | Description | Return Type | Example |
|----------|-------------|-------------|---------|
| `str(value)` | Convert to string | string | `str(42)` → `"42"` |
| `int(value)` | Convert to integer (truncates) | int | `int(3.7)` → `3`, `int("42")` → `42` |
| `float(value)` | Convert to float | float | `float("3.14")` → `3.14`, `float(5)` → `5.0` |
| `bool(value)` | Convert to boolean | bool | `bool(1)` → `True`, `bool(0)` → `False` |

### String Functions

| Function | Description | Return Type | Example |
|----------|-------------|-------------|---------|
| `len(s)` | Length of string | int | `len("hello")` → `5` |
| `concat(args...)` | Concatenate strings | `concat("a", "b", "c")` → `"abc"` |
| `upper(s)` | Convert to uppercase | `upper("hello")` → `"HELLO"` |
| `lower(s)` | Convert to lowercase | `lower("HELLO")` → `"hello"` |
| `strip(s)` | Trim whitespace (both sides) | `strip("  hi  ")` → `"hi"` |
| `trim(s)` | Alias for `strip` | `trim("  hi  ")` → `"hi"` |
| `replace(s, old, new)` | Replace all occurrences | `replace("foo", "o", "a")` → `"faa"` |
| `split(s, sep)` | Split and return first part | `split("a,b,c", ",")` → `"a"` |
| `substr(s, start, [end])` | Extract substring | `substr("hello", 1, 4)` → `"ell"` |
| `substring(s, start, [end])` | Alias for `substr` | `substring("hello", 0, 2)` → `"he"` |

### Math Functions

| Function | Description | Example |
|----------|-------------|---------|
| `abs(n)` | Absolute value | `abs(-5)` → `5` |
| `round(n, [digits])` | Round to specified digits | `round(3.14159, 2)` → `3.14` |
| `min(args...)` | Minimum value | `min(3, 1, 4)` → `1` |
| `max(args...)` | Maximum value | `max(3, 1, 4)` → `4` |

### Datetime Functions

#### Epoch Extraction (datetime to int)

These functions convert a datetime to an integer representing the count since Unix epoch (1970-01-01).

| Function | Description | Return Type | Example |
|----------|-------------|-------------|---------|
| `seconds(dt)` | Unix seconds | int | `seconds(order_date)` |
| `minutes(dt)` | Minutes since epoch | int | `minutes(order_date)` |
| `hours(dt)` | Hours since epoch | int | `hours(order_date)` |
| `days(dt)` | Days since epoch | int | `days(order_date)` |
| `weeks(dt)` | Weeks since epoch | int | `weeks(order_date)` |
| `months(dt)` | Months since epoch | int | `months(order_date)` |
| `quarters(dt)` | Quarters since epoch | int | `quarters(order_date)` |
| `years(dt)` | Years since epoch (year - 1970) | int | `years(order_date)` |

#### Date Arithmetic

| Function | Description | Example |
|----------|-------------|---------|
| `date_diff(end, start)` | Difference as duration | `date_diff(end_time, start_time)` |
| `date_diff(end, start, unit)` | Difference as number | `date_diff(end_time, start_time, "hours")` |
| `date_add(dt, dur)` | Add duration to datetime | `date_add(order_date, duration(7, "days"))` |
| `date_sub(dt, dur)` | Subtract duration from datetime | `date_sub(due_date, duration(1, "week"))` |

**Units for `date_diff`:** `nanoseconds`/`ns`, `microseconds`/`us`, `milliseconds`/`ms`, `seconds`/`s`, `minutes`/`m`, `hours`/`h`, `days`/`d`, `weeks`/`w`

### Duration Functions

#### Creating Durations

| Function | Description | Example |
|----------|-------------|---------|
| `duration(string)` | Parse duration string | `duration("2h30m")`, `duration("3d4h")` |
| `duration(value, unit)` | Create from value and unit | `duration(90, "minutes")` |

**Units:** `nanoseconds`/`ns`, `microseconds`/`us`/`µs`, `milliseconds`/`ms`, `seconds`/`s`, `minutes`/`m`, `hours`/`h`, `days`/`d`, `weeks`/`w`

**Duration string format:** Go-style with optional days prefix: `3d2h30m15s`, `2h30m`, `500ms`, `-1h30m`

#### Duration Extraction (duration to number)

| Function | Description | Example |
|----------|-------------|---------|
| `as_nanoseconds(dur)` | Total nanoseconds | `as_nanoseconds(elapsed)` |
| `as_microseconds(dur)` | Total microseconds | `as_microseconds(elapsed)` |
| `as_milliseconds(dur)` | Total milliseconds | `as_milliseconds(elapsed)` |
| `as_seconds(dur)` | Total seconds | `as_seconds(elapsed)` |
| `as_minutes(dur)` | Total minutes | `as_minutes(elapsed)` |
| `as_hours(dur)` | Total hours | `as_hours(elapsed)` |
| `as_days(dur)` | Total days | `as_days(elapsed)` |
| `format_duration(dur)` | Format as string | `format_duration(elapsed)` → `"2h30m"` |

## String Methods

String values support method calls using dot notation.

### Text Transformation

| Method | Description | Example |
|--------|-------------|---------|
| `.upper()` | Uppercase | `name.upper()` |
| `.lower()` | Lowercase | `name.lower()` |
| `.strip()` | Trim whitespace (both) | `name.strip()` |
| `.trim()` | Alias for `.strip()` | `name.trim()` |
| `.lstrip()` | Trim left whitespace | `name.lstrip()` |
| `.rstrip()` | Trim right whitespace | `name.rstrip()` |
| `.capitalize()` | Capitalize first letter | `"hello".capitalize()` → `"Hello"` |
| `.title()` | Title case | `"hello world".title()` → `"Hello World"` |

### String Operations

| Method | Description | Example |
|--------|-------------|---------|
| `.replace(old, new)` | Replace all occurrences | `name.replace("a", "b")` |
| `.split(sep)` | Split and return first part | `path.split("/")` |
| `.startswith(prefix)` | Check prefix | `name.startswith("Mr")` |
| `.endswith(suffix)` | Check suffix | `file.endswith(".csv")` |
| `.contains(sub)` | Check contains | `name.contains("smith")` |
| `.count(sub)` | Count occurrences | `text.count("the")` |
| `.find(sub)` | Find first index (-1 if not found) | `text.find("word")` |
| `.index(sub)` | Alias for `.find()` | `text.index("word")` |
| `.rfind(sub)` | Find last index | `path.rfind("/")` |
| `.rindex(sub)` | Alias for `.rfind()` | `path.rindex("/")` |

### Character Type Checks

| Method | Description | Example |
|--------|-------------|---------|
| `.isdigit()` | All characters are digits | `"123".isdigit()` → `True` |
| `.isalpha()` | All characters are letters | `"abc".isalpha()` → `True` |
| `.isalnum()` | All characters are alphanumeric | `"abc123".isalnum()` → `True` |
| `.isupper()` | Has uppercase, no lowercase | `"ABC".isupper()` → `True` |
| `.islower()` | Has lowercase, no uppercase | `"abc".islower()` → `True` |

## Column References

Reference column values by name as identifiers:

```
price * quantity
upper(customer_name)
order_date > ship_date
```

Column names with special characters should be quoted (not yet supported).

## Examples

### Computed Columns

```
# Total price calculation
price * quantity

# Concatenate first and last name
first_name + " " + last_name

# Check if order is late
ship_date > due_date

# Calculate order processing time
date_diff(ship_date, order_date, "hours")

# Categorize by value
amount > 1000

# String manipulation
upper(substr(country_code, 0, 2))
```

### Filtering Expressions

```
# High-value orders
amount > 1000 and status == "completed"

# Recent orders
days(order_date) > days(order_date) - 30

# Name search
customer_name.contains("Smith") or customer_name.startswith("Dr")
```
