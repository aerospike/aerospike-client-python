.. _llist:

.. currentmodule:: aerospike

=============================================
Large Ordered List Class --- :class:`LList`
=============================================

:class:`LList`
===============

.. class:: LList

    Large Ordered List (LList) is a collection of elements sorted by ``key`` \
    order, which is capable of growing unbounded. There are two ways in which \
    an element is sorted and located:

    .. hlist::
        :columns: 1

        * **Implied key** for the types :class:`str` and :class:`int`, where the value itself is used as the element's key.
        * **Explicit key** for :class:`dict` where a key named ``key`` must exist, and will be used to identify the element. A :class:`list` acts as a batch of similarly-typed elements, either :class:`str`, :class:`int`, or :class:`dict`.

    Example::

        from __future__ import print_function
        import aerospike
        from aerospike.exception import *
        import sys

        config = { 'hosts': [ ('127.0.0.1', 3000) ] }
        try:
            client = aerospike.client(config).connect()
        except ClientError as e:
            print("Error: {0} [{1}]".format(e.msg, e.code))
            sys.exit(1)

        key = ('test', 'articles', 'The Number One Soft Drink')
        tags = client.llist(key, 'tags')
        try:
            tags.add("soda")
            tags.add_many(["slurm","addictive","prizes"])
        except LDTError as e:
            print("Error: {0} [{1}]".format(e.msg, e.code))

        print(tags.find_first(2))
        print(tags.find_last(3))
        print(tags.find_from("addictive", 2))

        try:
            tags.remove("prizes")
        except:
            pass
        client.close()


    .. seealso::
        `Large Ordered List
        <https://www.aerospike.com/docs/guide/llist.html>`_.

    .. versionadded:: 1.0.45


    .. method:: add(element[, policy])

        Add an element to the :class:`LList`.

        :param element: the element to add to the large ordered list.
        :type element: one of :class:`str`, :class:`int`, :class:`list`, :class:`dict`
        :param dict policy: optional :ref:`aerospike_apply_policies`.
        :raises: subclass of :exc:`~aerospike.exception.LDTError`.

        .. note:: All elements in a specific large list must be of the same \
                  type, subsequent to the first element which sets the \
                  *key type* of the LList.

        .. code-block:: python

            key = ('test', 'articles', 'star-trek-vs-star-wars')
            comments = client.llist(key, 'comments')
            comments.add({'key':'comment-134', 'user':'vulcano', 'parent':'comment-101', 'body': 'wrong!'})


    .. method:: add_many(elements[, policy])

        Add a :class:`list` of elements to the :class:`LList`.

        :param list elements: a list of elements to add to the large ordered list.
        :param dict policy: optional :ref:`aerospike_apply_policies`.
        :raises: subclass of :exc:`~aerospike.exception.LDTError`.

        .. note:: All elements in a specific large list must be of the same \
                  type, subsequent to the first element which sets the \
                  *key type* of the LList.

    .. method:: remove(value[, policy])

        Remove an element from the :class:`LList`.

        :param value: the value identifying the element to remove from the \
            large ordered list. If the type of the elements in the \
            :class:`LList` is an aerospike ``map`` you need to provide a \
            :class:`dict` with a key ``key`` to explicitly identify the element.
        :type value: one of :class:`str`, :class:`int`, :class:`dict`
        :param dict policy: optional :ref:`aerospike_apply_policies`.
        :raises: subclass of :exc:`~aerospike.exception.LDTError`.

        .. code-block:: python

            key = ('test', 'articles', 'star-trek-vs-star-wars')
            comments = client.llist(key, 'comments')
            comments.remove({'key':'comment-134'})
            tags = client.llist(key, 'tags')
            tags.remove("tlhIngan Hol")

    .. method:: get(value[, policy]) -> element

        Get an element from the :class:`LList`.

        :param value: the value identifying the element to get from the \
            large ordered list. If the type of the elements in the LList is \
            an aerospike ``map`` you need to provide a :class:`dict` with a \
            key ``key`` whose value will be used to explicitly identify the element.
        :type value: one of :class:`str`, :class:`int`, :class:`dict`
        :param dict policy: optional :ref:`aerospike_apply_policies`.
        :rtype: one of :class:`str`, :class:`int`, :class:`dict`.
        :raises: subclass of :exc:`~aerospike.exception.LDTError`.

        .. code-block:: python

            key = ('test', 'articles', 'star-trek-vs-star-wars')
            comments = client.llist(key, 'comments')
            parent_comment = comments.get({'key':'comment-101'})


    .. method:: find_first(count[, policy]) -> [elements]

        Get the first *count* elements in the :class:`LList`.

        :param int count: the number of elements to return from the beginning of the list.
        :param dict policy: optional :ref:`aerospike_apply_policies`.
        :return: a :class:`list` of elements.
        :raises: subclass of :exc:`~aerospike.exception.LDTError`.


    .. method:: find_last(count[, policy]) -> [elements]

        Get the last *count* elements in the :class:`LList`.

        :param int count: the number of elements to return from the end of the list.
        :param dict policy: optional :ref:`aerospike_apply_policies`.
        :return: a :class:`list` of elements.
        :raises: subclass of :exc:`~aerospike.exception.LDTError`.


    .. method:: find_from(value, count[, policy]) -> [elements]

        Get *count* elements from the :class:`LList`, starting with the \
        element that matches the specified *value*.

        :param value: the value identifying the element from which to start \
            retrieving *count* elements. If the type of the elements in the \
            :class:`LList` is an aerospike ``map`` you need to provide a \
            :class:`dict` with a key ``key`` to explicitly identify this element.
        :type value: one of :class:`str`, :class:`int`, :class:`dict`
        :param int count: the number of elements to return from the end of the list.
        :param dict policy: optional :ref:`aerospike_apply_policies`.
        :return: a :class:`list` of elements.
        :raises: subclass of :exc:`~aerospike.exception.LDTError`.


    .. method:: size([policy]) -> int

        Get the number of elements in the :class:`LList`.

        :param dict policy: optional :ref:`aerospike_apply_policies`.
        :rtype: :class:`int`
        :raises: subclass of :exc:`~aerospike.exception.LDTError`.


    .. method:: set_page_size(size, [policy])

        Set the page size for this :class:`LList`.

        :param int size:
        :param dict policy: optional :ref:`aerospike_apply_policies`.
        :raises: subclass of :exc:`~aerospike.exception.LDTError`.

        .. warning::

            Requires server version >= 3.5.8


    .. method:: destroy([policy])

        Destroy the entire :class:`LList`.

        :param dict policy: optional :ref:`aerospike_apply_policies`.
        :raises: subclass of :exc:`~aerospike.exception.LDTError`.

