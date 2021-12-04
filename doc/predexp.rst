.. _aerospike.predexp:

********************************************************
:mod:`aerospike.predexp` --- Query Predicate Expressions
********************************************************

.. module:: aerospike.predexp
    :platform: 64-bit Linux and OS X
    :synopsis: Helper functions to generate Predicate Expression filters

The following methods allow a user to define a predicate expression filter. Predicate expression filters are applied on the query results on the server. Predicate expression filters may occur on any bin in the record.

.. py:function:: predexp_and(nexpr)

    Create an AND logical predicate expression. 

    :param nexpr: :class:`int` Number of expressions to combine with "and" . The value of nexpr must be between 1 and 65535.
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records where the value of bin "c" is between 11 and 20 inclusive:

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.integer_bin("c"),
            predexp.integer_value(11),
            predexp.integer_greatereq(),
            predexp.integer_bin("c"),
            predexp.integer_value(20),
            predexp.integer_lesseq(),
            predexp.predexp_and(2)
        ]

.. py:function:: predexp_or(nexpr)

    Create an Or logical predicate expression.

    :param nexpr: :class:`int` Number of expressions to combine with "or". The value of nexpr must be between 1 and 65535.
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records where the value of bin "pet" is "dog" or "cat"

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.string_bin("pet"),
            predexp.string_value("cat"),
            predexp.string_equal(),
            predexp.string_bin("pet"),
            predexp.string_value("dog"),
            predexp.string_equal(),
            predexp.predexp_or(2)
        ]

.. py:function:: predexp_not()

    Create a not logical predicate expression which negates the previouse predicate expression on the stack.

    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records where the value of bin "pet" is not "cat"

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.string_bin("pet"),
            predexp.string_value("cat"),
            predexp.string_equal(),
            predexp.predexp_not()
        ]

.. py:function:: integer_bin(bin_name)

    Create an integer bin value predicate expression.

    :param bin_name: :class:`str` The name of the bin containing an integer.
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records where the value of bin "age" is 42

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.integer_bin("age"),
            predexp.integer_value(42),
            predexp.integer_equal()
        ]

.. py:function:: string_bin(bin_name)

    Create a string bin value predicate expression.

    :param bin_name: :class:`str` The name of the bin containing a string.
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records where the value of bin "name" is "Bob".

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.string_bin("name"),
            predexp.string_value("Bob"),
            predexp.string_equal()
        ]

.. py:function:: geojson_bin(bin_name)

    Create a GeoJSON bin value predicate expression.

    :param bin_name: :class:`str` The name of the bin containing a GeoJSON value.
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records where the value of bin "location" is within a specified region.

    .. code-block :: python

        from aerospike import predexp as predexp
        geo_region = aerospike.GeoJSON(
            {"type": "AeroCircle", "coordinates": [[-122.0, 37.5], 1000]}).dumps()
        predexps =  [
            predexp.geojson_bin("location"),
            predexp.geojson_value(geo_region),
            predexp.geojson_within()
        ]

.. py:function:: list_bin(bin_name)

    Create a list bin value predicate expression.

    :param bin_name: :class:`str` The name of the bin containing a list.
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records where the list in bin "names" contains an entry equal to "Alice"

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.string_var("list_entry"),
            predexp.string_value("Alice"),
            predexp.string_equal(),
            predexp.list_bin("names"),
            predexp.list_iterate_or("list_entry")
        ]

.. py:function:: map_bin(bin_name)

    Create a map bin value predicate expression.

    :param bin_name: :class:`str` The name of the bin containing a map value.
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records where the map in bin "pet_count" has an entry with a key equal to "Cat"

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.string_var("map_key"),
            predexp.string_value("Cat"),
            predexp.string_equal(),
            predexp.map_bin("pet_count"),
            predexp.mapkey_iterate_or("map_key")
        ]

.. py:function:: geojson_value(geo_value)

    Create a GeoJSON value predicate expression.

    :param bin_name: :class:`str` The geojson string.
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records where the value of bin "location" is within a specified region.

    .. code-block :: python

        from aerospike import predexp as predexp
        geo_region = aerospike.GeoJSON(
            {"type": "AeroCircle", "coordinates": [[-122.0, 37.5], 1000]}).dumps()
        predexps =  [
            predexp.geojson_bin("location"),
            predexp.geojson_value(geo_region),
            predexp.geojson_within()
        ]

.. py:function:: integer_value(int_value)

    Create an integer value predicate expression.

    :param bin_name: :class:`int` The integer value
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records where the value of bin "age" is 42

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.integer_bin("age"),
            predexp.integer_value(42),
            predexp.integer_equal()
        ]

.. py:function:: string_value(string_value)

    Create a string value predicate expression.

    :param bin_name: :class:`str` The string value.
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records where the value of bin "name" is "Bob".

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.string_bin("name"),
            predexp.string_value("Bob"),
            predexp.string_equal()
        ]

.. py:function:: integer_var(var_name)

    Create an integer iteration variable predicate expression.

    :param var_name: :class:`str` The name of the variable. This should match a value used when specifying the iteration.
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example the following selects a record where the list in bin "numbers" contains an entry equal to ``42``

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.integer_var("item"),
            predexp.integer_value(42),
            predexp.integer_equal(),
            predexp.list_bin("numbers"),
            predexp.list_iterate_or("item")
        ]

.. py:function:: string_var(var_name)

    Create an string iteration variable predicate expression.

    :param var_name: :class:`str` The name of the variable. This should match a value used when specifying the iteration.
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example the following selects a record where the list in bin "languages" contains an entry equal to ``"Python"``

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.string_var("item"),
            predexp.string_value("Python"),
            predexp.string_equal(),
            predexp.list_bin("languages"),
            predexp.list_iterate_or("item")
        ]

.. py:function:: geojson_var(var_name)

    Create an GeoJSON iteration variable predicate expression.

    :param var_name: :class:`str` The name of the variable. This should match a value used when specifying the iteration.
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

.. py:function:: list_iterate_or(var_name)

    Create an list iteration OR logical predicate expression.

    :param bin_name: :class:`str` The name of the iteration variable
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    The list iteration expression pops two children off the expression stack. The left child (pushed earlier) must contain a logical subexpression
    containing one or more matching iteration variable expressions.  The right child (pushed later) must specify a list bin. The list iteration traverses the list
    and repeatedly evaluates the subexpression substituting each list element's value into the matching iteration variable.
    The result of the iteration expression is a logical OR of all of the individual element evaluations.

    If the list bin contains zero elements :meth:`list_iterate_or` will evaluate to false.

    For example, the following sequence of predicate expressions selects records where the list in bin "names" contains an entry equal to "Alice"

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.string_var("list_entry"),
            predexp.string_value("Alice"),
            predexp.string_equal(),
            predexp.list_bin("names"),
            predexp.list_iterate_or("list_entry")
        ]

.. py:function:: list_iterate_and(var_name)

    Create an list iteration And logical predicate expression.

    :param var_name: :class:`str` The name of the iteration variable
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    The list iteration expression pops two children off the expression stack. The left child (pushed earlier) must contain a logical subexpression
    containing one or more matching iteration variable expressions.  The right child (pushed later) must specify a list bin. The list iteration traverses the list
    and repeatedly evaluates the subexpression substituting each list element's value into the matching iteration variable.
    The result of the iteration expression is a logical AND of all of the individual element evaluations.

    If the list bin contains zero elements :meth:`list_iterate_and` will evaluate to true. This is useful when testing for exclusion (see example).

    For example, the following sequence of predicate expressions selects records where the list in bin "names" contains no entries equal to "Bob".

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.string_var("list_entry"),
            predexp.string_value("Bob"),
            predexp.string_equal(),
            predexp.predexp_not(),
            predexp.list_bin("names"),
            predexp.list_iterate_and("list_entry")
        ]

.. py:function:: mapkey_iterate_or(var_name)

    Create an map key iteration Or logical predicate expression.

    :param var_name: :class:`str` The name of the iteration variable
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    The mapkey iteration expression pops two children off the expression stack.
    The left child (pushed earlier) must contain a logical subexpression containing one or more matching iteration variable expressions.
    The right child (pushed later) must specify a map bin.
    The mapkey iteration traverses the map and repeatedly evaluates the subexpression substituting each map key value into The matching iteration variable.
    The result of the iteration expression is a logical OR of all of the individual element evaluations.

    If the map bin contains zero elements :meth:`mapkey_iterate_or` will return false.
    For example, the following sequence of predicate expressions selects records where the map in bin "pet_count" has an entry with a key equal to "Cat"

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.string_var("map_key"),
            predexp.string_value("Cat"),
            predexp.string_equal(),
            predexp.map_bin("pet_count"),
            predexp.mapkey_iterate_or("map_key")
        ]

.. py:function:: mapkey_iterate_and(var_name)

    Create an map key iteration AND logical predicate expression.

    :param var_name: :class:`str` The name of the iteration variable
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    The mapkey iteration expression pops two children off the expression stack.
    The left child (pushed earlier) must contain a logical subexpression containing one or more matching iteration variable expressions.
    The right child (pushed later) must specify a map bin.
    The mapkey iteration traverses the map and repeatedly evaluates the subexpression substituting each map key value into The matching iteration variable.
    The result of the iteration expression is a logical AND of all of the individual element evaluations.

    If the map bin contains zero elements :meth:`mapkey_iterate_and` will return true. This is useful when testing for exclusion (see example).

    For example, the following sequence of predicate expressions selects records where the map in bin "pet_count" does not contain an entry with a key equal to "Cat".

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.string_var("map_key"),
            predexp.string_value("Cat"),
            predexp.string_equal(),
            predexp.predexp_not(),
            predexp.map_bin("pet_count"),
            predexp.mapkey_iterate_and("map_key")
        ]

.. py:function:: mapval_iterate_or(var_name)

    Create an map value iteration Or logical predicate expression.

    :param var_name: :class:`str` The name of the iteration variable
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    The mapval iteration expression pops two children off the expression stack.
    The left child (pushed earlier) must contain a logical subexpression containing one or more matching iteration variable expressions.
    The right child (pushed later) must specify a map bin.
    The mapval iteration traverses the map and repeatedly evaluates the subexpression substituting each map value into the matching iteration variable.
    The result of the iteration expression is a logical OR of all of the individual element evaluations.

    If the map bin contains zero elements :meth:`mapval_iterate_or` will return false.

    For example, the following sequence of predicate expressions selects records where at least one of the values in the map in bin "pet_count" is ``0``

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.string_var("map_key"),
            predexp.integer_value(0),
            predexp.integer_equal(),
            predexp.map_bin("pet_count"),
            predexp.mapval_iterate_or("map_key")
        ]

.. py:function:: mapval_iterate_and(var_name)

    Create an map value iteration AND logical predicate expression.

    :param var_name: :class:`str` The name of the iteration variable
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    The mapval iteration expression pops two children off the expression stack.
    The left child (pushed earlier) must contain a logical subexpression containing one or more matching iteration variable expressions.
    The right child (pushed later) must specify a map bin.
    The mapval iteration traverses the map and repeatedly evaluates the subexpression substituting each map value into the matching iteration variable.
    The result of the iteration expression is a logical AND of all of the individual element evaluations.

    If the map bin contains zero elements :meth:`mapval_iterate_and` will return true. This is useful when testing for exclusion (see example).

    For example, the following sequence of predicate expressions selects records where none of the values in the map in bin "pet_count" is ``0``

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.string_var("map_key"),
            predexp.integer_value(0),
            predexp.integer_equal(),
            predexp.predexp_not(),
            predexp.map_bin("pet_count"),
            predexp.mapval_iterate_and("map_key")
        ]

.. py:function:: rec_digest_modulo(mod)

    Create a digest modulo record metadata value predicate expression.

    :param mod: :class:`int` The value of this expression assumes the value of 4 bytes of the digest modulo this argument.
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records that have ``digest(key) % 3 == 1`` :

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.rec_digest_modulo(3),
            predexp.integer_value(1),
            predexp.integer_equal()
        ]

.. py:function:: rec_last_update()

    Create a last update record metadata value predicate expression. The record last update expression assumes the value of the number of nanoseconds since the unix epoch that the record was last updated.

    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records that have been updated after a timestamp:

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.rec_last_update(),
            predexp.integer_value(timestamp_ns),
            predexp.integer_greater()
        ]

.. py:function:: rec_void_time()

    Create a void time record metadata value predicate expression. The record void time expression assumes the value of the number of nanoseconds since the unix epoch when the record will expire. The special value of 0 means the record will not expire.

    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records that have void time set to 0 (no expiration):

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.rec_void_time(),
            predexp.integer_value(0),
            predexp.integer_equal()
        ]

.. py:function:: rec_device_size()

    Create a record device size metadata value predicate expression. The record device size expression assumes the value of the size in bytes that the record occupies on device storage. For non-persisted records, this value is 0.

    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records whose device storage size is larger than 65K:

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.rec_device_size(),
            predexp.integer_value(65 * 1024),
            predexp.integer_greater()
        ]

.. py:function:: integer_equal()

    Create an integer comparison logical predicate expression.
    If the value of either of the child expressions is unknown because a specified bin does not exist or contains a value of the wrong type the result of the comparison is false. If a true outcome is desirable in this situation use the complimentary comparison and enclose in a logical NOT.

    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records that have bin "foo" equal to 42:

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.integer_bin("foo"),
            predexp.integer_value(42),
            predexp.integer_equal()
        ]

.. py:function:: integer_greater()

    Create an integer comparison logical predicate expression.

    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records that have bin "foo" greater than 42:

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.integer_bin("foo"),
            predexp.integer_value(42),
            predexp.integer_greater()
        ]

.. py:function:: integer_greatereq()

    Create an integer comparison logical predicate expression.

    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records that have bin "foo" greater than or equal to 42:

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.integer_bin("foo"),
            predexp.integer_value(42),
            predexp.integer_greatereq()
        ]

.. py:function:: integer_less()

    Create an integer comparison logical predicate expression.

    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records that have bin "foo" less than 42:

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.integer_bin("foo"),
            predexp.integer_value(42),
            predexp.integer_less()
        ]

.. py:function:: integer_lesseq()

    Create an integer comparison logical predicate expression.

    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records that have bin "foo" less than or equal to 42:

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.integer_bin("foo"),
            predexp.integer_value(42),
            predexp.integer_lesseq()
        ]

.. py:function:: integer_unequal()

    Create an integer comparison logical predicate expression.

    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    This expression will evaluate to true if, and only if, both children of the expression exist, and are of type integer, and are not equal to each other.
    If this is not desired, utilize :meth:`aerospike.predexp.integer_equal` in conjunction with :meth:`aerospike.predexp.predexp_not`.

    For example, the following sequence of predicate expressions selects records that have bin "foo" not equal to 42:


    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.integer_bin("foo"),
            predexp.integer_value(42),
            predexp.integer_unequal()
        ]

.. py:function:: string_equal()

    Create an integer comparison logical predicate expression.
    If the value of either of the child expressions is unknown because a specified bin does not exist or contains a value of the wrong type the result of the comparison is false. If a true outcome is desirable in this situation use the complimentary comparison and enclose in a logical NOT.

    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records that have bin "foo" equal to "bar":

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.string_bin("foo"),
            predexp.string_value("bar"),
            predexp.string_equal()
        ]

.. py:function:: string_unequal()

    Create an integer comparison logical predicate expression.

    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    This expression will evaluate to true if, and only if, both children of the expression exist, and are of type string, and are not equal to each other.
    If this is not desired, utilize :meth:`aerospike.predexp.string_equal` in conjunction with :meth:`aerospike.predexp.predexp_not`.

    For example, the following sequence of predicate expressions selects records that have bin "foo" not equal to "bar":

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.string_bin("foo"),
            predexp.string_value("bar"),
            predexp.string_unequal()
        ]

.. py:function:: geojson_within()

    Create a Geojson within predicate expression.

    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.geojson_bin("location"),
            predexp.geojson_value(my_geo_region),
            predexp.geojson_within()
        ]

.. py:function:: geojson_contains()

    Create a Geojson contains predicate expression.

    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps =  [
            predexp.geojson_bin("region"),
            predexp.geojson_value(my_geo_point),
            predexp.geojson_contains()
        ]

.. py:function:: string_regex(*flags)

    Create a string regex predicate. May be called without any arguments to specify default behavior.

    :param flags: :class:`int` :ref:`regex_constants` Any, or none of the aerospike REGEX constants
    :return: `tuple` to be used in :meth:`aerospike.Query.predexp`.

    For example, the following sequence of predicate expressions selects records that have bin "hex" value ending in '1' or '2':

    .. code-block :: python

        from aerospike import predexp as predexp
        predexps = [
            predexp.string_bin('hex'),
            predexp.string_value('0x00.[12]'),
            predexp.string_regex(aerospike.REGEX_ICASE)
        ]
